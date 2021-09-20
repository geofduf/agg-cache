package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/bits"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/geofduf/logging"
)

const (
	missingFlag byte = iota
	regularFlag byte = iota
	encodedFlag byte = iota
	entryFlag   byte = iota
)

type levels []int

func (l *levels) String() string {
	var s []string
	for _, v := range *l {
		s = append(s, strconv.Itoa(v))
	}
	return strings.Join(s, ",")
}

func (l *levels) Set(value string) error {
	*l = nil
	processed := make(map[int]bool)
	for _, level := range strings.Split(value, ",") {
		v, err := strconv.Atoi(level)
		if err != nil || v <= 0 {
			return fmt.Errorf(`"%s" is not a positive integer`, level)
		}
		if !processed[v] {
			*l = append(*l, v)
			processed[v] = true
		}
	}
	sort.Ints(*l)
	return nil
}

var logger *logging.Logger

var config struct {
	aggs      levels
	frequency int
	logging   int
	listen    string
}

type garbageCollector struct {
	groups map[int]struct{}
	keys   []map[int]struct{}
}

type input struct {
	Group  string   `json:"group"`
	Key    string   `json:"key"`
	Values []*int64 `json:"values"`
}

type response struct {
	Status string                 `json:"status"`
	Data   map[string]*storeEntry `json:"data"`
}

type storeEntry struct {
	Values   []int64  `json:"values"`
	Counters []uint32 `json:"counters"`
	Cnt      uint32   `json:"-"`
}

type groupInfo struct {
	name    string
	forward map[string]int
	reverse []string
}

type mStore struct {
	sync.RWMutex
	data    [][][]*storeEntry
	forward map[string]int
	reverse []*groupInfo
}

type mQueue struct {
	sync.Mutex
	data map[int][]input
}

type application struct {
	store mStore
	queue mQueue
}

func (app *application) insertHandler(w http.ResponseWriter, req *http.Request) {

	var (
		entries []input
		message string
		status  string = "ok"
	)

	d := json.NewDecoder(req.Body)
	if err := d.Decode(&entries); err != nil {
		status = "error"
		message = "cannot decode request body"
		logger.Warning("INS", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
	} else {
		ts := int(time.Now().Unix()) / config.frequency * config.frequency
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

func (app *application) groupHandler(w http.ResponseWriter, req *http.Request) {

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
			for i, v := range config.aggs {
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
			logger.Error("GET", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
		}
	} else {
		message = "invalid request"
		logger.Warning("GET", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
		fmt.Fprint(w, `{"status": "error", "data": {}}`)
	}

}

func (app *application) processData() {

	aggs := config.aggs
	aggsLength := len(aggs)
	rawData := make(map[int][]byte)
	container := make([]byte, binary.MaxVarintLen64)
	gc := garbageCollector{groups: make(map[int]struct{})}
	var buf bytes.Buffer

	index := make(map[int]map[int]bool)
	for _, agg := range aggs {
		index[agg] = make(map[int]bool)
	}

	napDuration := config.frequency - int(time.Now().Unix())%config.frequency
	logger.System("PRO", fmt.Sprintf("scheduler will start in %d second(s)", napDuration))
	time.Sleep(time.Duration(napDuration) * time.Second)

	ticker := time.NewTicker(time.Duration(config.frequency) * time.Second)

	for {
		t := <-ticker.C
		start := time.Now()

		if int(start.Unix())%config.frequency > 3 {
			logger.Warning("PRO", "probably drifting")
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
			logger.Warning("PRO", fmt.Sprintf("offset of %d seconds", elapsed/1000000000))
		}

		ts := int(time.Now().Unix()) / config.frequency * config.frequency

		for _, bucket := range buckets {
			for aggIndex := range aggs {
				if bucket >= ts-aggs[aggIndex] {
					buf.Reset()
					for _, v := range queue[bucket] {
						var groupId, keyId int
						var found bool
						previousValue := new(int64)
						if groupId, found = app.store.forward[v.Group]; !found {
							g := &groupInfo{
								name:    v.Group,
								forward: map[string]int{v.Key: 0},
								reverse: []string{v.Key},
							}
							if len(gc.groups) > 0 {
								for groupId = range gc.groups {
									delete(gc.groups, groupId)
									break
								}
								app.store.reverse[groupId] = g
								app.store.data[groupId] = [][]*storeEntry{make([]*storeEntry, aggsLength)}
							} else {
								groupId = len(app.store.reverse)
								app.store.reverse = append(app.store.reverse, g)
								app.store.data = append(app.store.data, [][]*storeEntry{make([]*storeEntry, aggsLength)})
								gc.keys = append(gc.keys, make(map[int]struct{}))
							}
							app.store.forward[v.Group] = groupId
						} else if keyId, found = app.store.reverse[groupId].forward[v.Key]; !found {
							g := app.store.reverse[groupId]
							if len(gc.keys[groupId]) > 0 {
								for keyId = range gc.keys[groupId] {
									delete(gc.keys[groupId], keyId)
									break
								}
								g.reverse[keyId] = v.Key
								app.store.data[groupId][keyId] = make([]*storeEntry, aggsLength)
							} else {
								keyId = len(g.reverse)
								g.reverse = append(g.reverse, v.Key)
								app.store.data[groupId] = append(app.store.data[groupId], make([]*storeEntry, aggsLength))
							}
							g.forward[v.Key] = keyId
						}
						if !found {
							for i := 0; i < aggsLength; i++ {
								app.store.data[groupId][keyId][i] = &storeEntry{
									Values:   make([]int64, len(v.Values)),
									Counters: make([]uint32, len(v.Values)),
								}
							}
						}
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
									if len(app.store.data[groupId]) == len(gc.keys[groupId])-1 {
										gc.groups[groupId] = struct{}{}
										gc.keys[groupId] = nil
										app.store.data[groupId] = nil
										delete(app.store.forward, app.store.reverse[groupId].name)
										app.store.reverse[groupId] = nil
									} else {
										gc.keys[groupId][keyId] = struct{}{}
										app.store.data[groupId][keyId] = nil
										delete(app.store.reverse[groupId].forward, app.store.reverse[groupId].reverse[keyId])
									}
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
		logger.Debug("PRO", fmt.Sprintf("ticker: %s duration: %s", t, time.Since(start)))
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

func main() {
	flag.Parse()
	if config.frequency <= 0 {
		logger.Fatal("INI", "frequency must be a positive integer")
	} else if config.frequency > config.aggs[0] {
		logger.Fatal("INI", "frequency must be lower or equal to the first aggregation level")
	}
	logger.SetLevel(config.logging)
	app := application{
		queue: mQueue{data: make(map[int][]input)},
		store: mStore{forward: make(map[string]int)},
	}
	go app.processData()
	http.HandleFunc("/group/", app.groupHandler)
	http.HandleFunc("/insert", app.insertHandler)
	if err := http.ListenAndServe(config.listen, nil); err != nil {
		logger.Fatal("INI", err.Error())
	}
}

func init() {
	logger = logging.GetLogger()
	config.aggs = levels{300, 1800, 3600, 7200, 14400, 86400}
	flag.StringVar(&config.listen, "listen", "127.0.0.1:8888", "HTTP listening address:port")
	flag.IntVar(&config.logging, "logging", 3, "Logging level")
	flag.IntVar(&config.frequency, "frequency", 300, "Data processing frequency in seconds")
	flag.Var(&config.aggs, "levels", "Comma-separated list of aggregation levels in seconds")
}
