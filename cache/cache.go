package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	missingFlag byte = iota
	regularFlag
	encodedFlag
	entryFlag
)

type input struct {
	Group  string   `json:"group"`
	Key    string   `json:"key"`
	Values []*int64 `json:"values"`
}

type response struct {
	Status string                 `json:"status"`
	Data   map[string]*storeEntry `json:"data"`
}

type queue struct {
	sync.Mutex
	data map[int][]input
}

type application struct {
	store     store
	queue     queue
	aggs      []int
	frequency int
	logger    loggerWrapper
}

func (app *application) InsertHandler(w http.ResponseWriter, req *http.Request) {

	var (
		entries []input
		message string
		status  string = "ok"
	)

	d := json.NewDecoder(req.Body)
	if err := d.Decode(&entries); err != nil {
		status = "error"
		message = "cannot decode request body"
		app.logger.dispatch(logLevelWarning, "INS", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
	} else {
		ts := int(time.Now().Unix()) / app.frequency * app.frequency
		app.queue.Lock()
		if _, ok := app.queue.data[ts]; ok {
			app.queue.data[ts] = append(app.queue.data[ts], entries...)
		} else {
			app.queue.data[ts] = entries
		}
		app.queue.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status": "%s", "message": "%s"}`, status, message)

}

func (app *application) GroupHandler(w http.ResponseWriter, req *http.Request) {

	var (
		group   string
		agg     int
		err     error
		message string
	)

	param := req.FormValue("aggregation")
	slugs := strings.Split(req.URL.Path, "/")
	valid := false

	if req.Method == http.MethodGet && len(slugs) == 3 {
		agg, err = strconv.Atoi(param)
		if err == nil {
			for i, v := range app.aggs {
				if agg == v {
					valid = true
					group = slugs[2]
					agg = i
					break
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")

	if valid {
		app.store.RLock()
		defer app.store.RUnlock()
		r := response{
			Status: "ok",
			Data:   make(map[string]*storeEntry),
		}
		if groupId, ok := app.store.forward[group]; ok {
			for keyId, v := range app.store.data[groupId] {
				if v != nil {
					r.Data[app.store.reverse[groupId].reverse[keyId]] = v[agg]
				}
			}
		}
		if err = json.NewEncoder(w).Encode(r); err != nil {
			message = "cannot build response"
			app.logger.dispatch(logLevelError, "GET", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
		}
	} else {
		message = "invalid request"
		app.logger.dispatch(logLevelWarning, "GET", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
		fmt.Fprint(w, `{"status": "error", "data": {}}`)
	}

}

func (app *application) processData() {

	aggs := app.aggs
	aggsLength := len(aggs)
	rawData := make(map[int][]byte)
	container := make([]byte, binary.MaxVarintLen64)
	var buf bytes.Buffer

	index := make(map[int]map[int]bool)
	for _, agg := range aggs {
		index[agg] = make(map[int]bool)
	}

	napDuration := app.frequency - int(time.Now().Unix())%app.frequency
	app.logger.dispatch(logLevelSystem, "PRO", fmt.Sprintf("scheduler will start in %d second(s)", napDuration))
	time.Sleep(time.Duration(napDuration) * time.Second)

	ticker := time.NewTicker(time.Duration(app.frequency) * time.Second)

	for {
		t := <-ticker.C
		start := time.Now()

		if int(start.Unix())%app.frequency > 3 {
			app.logger.dispatch(logLevelWarning, "PRO", "probably drifting")
		}

		app.queue.Lock()
		queue := app.queue.data
		app.queue.data = make(map[int][]input)
		app.queue.Unlock()

		buckets := make([]int, 0, len(queue))
		for k := range queue {
			buckets = append(buckets, k)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(buckets)))

		app.store.Lock()

		if elapsed := time.Since(start); elapsed > 1000000000 {
			app.logger.dispatch(logLevelWarning, "PRO", fmt.Sprintf("offset of %d seconds", elapsed/1000000000))
		}

		ts := int(time.Now().Unix()) / app.frequency * app.frequency

		for _, bucket := range buckets {
			for aggIndex := range aggs {
				if bucket >= ts-aggs[aggIndex] {
					buf.Reset()
					for _, v := range queue[bucket] {
						previousValue := new(int64)
						groupId, keyId := app.store.getIdentifiers(v.Group, v.Key, aggsLength, len(v.Values), app.logger)
						buf.WriteByte(entryFlag)
						n := binary.PutVarint(container, int64(groupId))
						buf.Write(container[:n])
						n = binary.PutVarint(container, int64(keyId))
						buf.Write(container[:n])
						for i := 0; i < len(v.Values); i++ {
							if v.Values[i] == nil {
								buf.WriteByte(missingFlag)
							} else {
								flag, value := tryDeltaEncoding(v.Values[i], previousValue)
								buf.WriteByte(flag)
								n = binary.PutVarint(container, value)
								buf.Write(container[:n])
								previousValue = v.Values[i]
								for j := aggIndex; j < aggsLength; j++ {
									if i == 0 {
										app.store.data[groupId][keyId][j].Cnt++
									}
									app.store.data[groupId][keyId][j].Values[i] += *v.Values[i]
									app.store.data[groupId][keyId][j].Counters[i]++
								}
							}
						}
					}
					for i := aggIndex; i < aggsLength; i++ {
						index[aggs[i]][bucket] = true
					}
					if _, ok := rawData[bucket]; ok {
						buf.Write(rawData[bucket])
					}
					rawData[bucket] = make([]byte, buf.Len())
					copy(rawData[bucket], buf.Bytes())
					break
				}
			}
		}

		for i, agg := range aggs {
			for bucket := range index[agg] {
				if bucket < ts-agg {
					var groupId, keyId, cnt, j int
					var previousValue int64
					var isValue, skip bool
					for cnt < len(rawData[bucket]) {
						if !isValue {
							switch rawData[bucket][cnt] {
							case entryFlag:
								j = 0
								skip = false
								cnt += 1
								id, n := binary.Varint(rawData[bucket][cnt:])
								groupId = int(id)
								cnt += n
								id, n = binary.Varint(rawData[bucket][cnt:])
								keyId = int(id)
								cnt += n
								if i == aggsLength-1 && app.store.data[groupId][keyId][i].Cnt == 1 {
									app.store.releaseKey(groupId, keyId, app.logger)
									skip = true
								} else {
									app.store.data[groupId][keyId][i].Cnt--
								}
							case missingFlag:
								j += 1
								cnt += 1
							case regularFlag, encodedFlag:
								isValue = true
								cnt += 1
							}
						} else {
							value, n := binary.Varint(rawData[bucket][cnt:])
							if rawData[bucket][cnt-1] == encodedFlag {
								value = value + previousValue
							}
							if !skip {
								app.store.data[groupId][keyId][i].Values[j] -= value
								app.store.data[groupId][keyId][i].Counters[j]--
							}
							isValue = false
							j += 1
							cnt += n
							previousValue = value
						}
					}
					delete(index[agg], bucket)
					if i == aggsLength-1 {
						delete(rawData, bucket)
					}
				}
			}
		}
		app.store.Unlock()
		app.logger.dispatch(logLevelDebug, "PRO", fmt.Sprintf("ticker: %s duration: %s store: %v", t, time.Since(start), app.store.statistics))
	}
}

func tryDeltaEncoding(value *int64, previousValue *int64) (byte, int64) {
	if x := *value; (x > 63 || x < -63) && previousValue != nil {
		y := x - *previousValue
		delta := y
		if x < 0 {
			x = -x
		}
		if y < 0 {
			y = -y
		}
		if x > y && bits.Len64(uint64(x))/7 > bits.Len64(uint64(y))/7 {
			return encodedFlag, delta
		}
	}
	return regularFlag, *value
}

func NewCache(frequency int, aggs []int, logger logger) (app *application, err error) {
	sort.Ints(aggs)
	if frequency <= 0 {
		err = errors.New("frequency must be a positive integer")
	} else if frequency > aggs[0] {
		err = errors.New("frequency must be lower or equal to the first aggregation level")
	} else {
		app = &application{
			queue:     queue{data: make(map[int][]input)},
			store:     store{forward: make(map[string]int), recycler: make(map[int]struct{})},
			aggs:      aggs,
			frequency: frequency,
			logger:    loggerWrapper{logger},
		}
		go app.processData()
	}
	return
}
