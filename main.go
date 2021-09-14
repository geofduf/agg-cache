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
	Values   []int64 `json:"values"`
	Counters []int   `json:"counters"`
	Cnt      int     `json:"-"`
}

type mapping struct {
	forward map[string]int64
	reverse map[int64]string
}

type mStore struct {
	sync.RWMutex
	data     map[string]map[string]map[int]*storeEntry
	groupMap mapping
	keyMap   mapping
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
			for _, v := range config.aggs {
				if agg == v {
					valid = true
					group = slugs[2]
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
		if _, ok := app.store.data[group]; ok {
			for k := range app.store.data[group] {
				r.Data[k] = app.store.data[group][k][agg]
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

	var groupCnt int64
	var keyCnt int64

	aggs := config.aggs
	aggsLength := len(aggs)
	rawData := make(map[int][]byte)
	container := make([]byte, binary.MaxVarintLen64)

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
		store := app.store.data

		if elapsed := time.Since(start); elapsed > 1000000000 {
			logger.Warning("PRO", fmt.Sprintf("offset of %d seconds", elapsed/1000000000))
		}

		ts := int(time.Now().Unix()) / config.frequency * config.frequency

		for _, bucket := range buckets {
			for aggIndex := range aggs {
				if bucket >= ts-aggs[aggIndex] {
					buf := new(bytes.Buffer)
					previousValue := new(int64)
					for _, v := range queue[bucket] {
						groupId, ok := app.store.groupMap.forward[v.Group]
						if !ok {
							app.store.groupMap.forward[v.Group] = groupCnt
							app.store.groupMap.reverse[groupCnt] = v.Group
							groupId = groupCnt
							groupCnt += 1
						}
						keyId, ok := app.store.keyMap.forward[v.Key]
						if !ok {
							app.store.keyMap.forward[v.Key] = keyCnt
							app.store.keyMap.reverse[keyCnt] = v.Key
							keyId = keyCnt
							keyCnt += 1
						}
						buf.WriteByte(entryFlag)
						n := binary.PutVarint(container, groupId)
						buf.Write(container[:n])
						n = binary.PutVarint(container, keyId)
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
							}
						}
						if _, ok := store[v.Group]; !ok {
							store[v.Group] = make(map[string]map[int]*storeEntry)
							store[v.Group][v.Key] = make(map[int]*storeEntry)
							for i := 0; i < aggsLength; i++ {
								store[v.Group][v.Key][aggs[i]] = &storeEntry{
									Values:   make([]int64, len(v.Values)),
									Counters: make([]int, len(v.Values)),
								}
							}
						} else if _, ok := store[v.Group][v.Key]; !ok {
							store[v.Group][v.Key] = make(map[int]*storeEntry)
							for i := 0; i < aggsLength; i++ {
								store[v.Group][v.Key][aggs[i]] = &storeEntry{
									Values:   make([]int64, len(v.Values)),
									Counters: make([]int, len(v.Values)),
								}
							}
						}
						for i := aggIndex; i < aggsLength; i++ {
							for j := range v.Values {
								if v.Values[j] != nil {
									store[v.Group][v.Key][aggs[i]].Values[j] += *v.Values[j]
									store[v.Group][v.Key][aggs[i]].Counters[j]++
								}
							}
							store[v.Group][v.Key][aggs[i]].Cnt++
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

		var isValue, skip bool
		var j, cnt int
		var group, key string

		for i, agg := range aggs {
			for bucket := range index[agg] {
				if bucket < ts-agg {
					cnt = 0
					var previousValue int64
					for cnt < len(rawData[bucket]) {
						if !isValue {
							switch rawData[bucket][cnt] {
							case entryFlag:
								j = 0
								skip = false
								cnt += 1
								id, n := binary.Varint(rawData[bucket][cnt:])
								group = app.store.groupMap.reverse[id]
								cnt += n
								id, n = binary.Varint(rawData[bucket][cnt:])
								key = app.store.keyMap.reverse[id]
								cnt += n
								if i == aggsLength-1 && store[group][key][agg].Cnt == 1 {
									delete(store[group], key)
									if len(store[group]) == 0 {
										delete(store, group)
									}
									skip = true
								} else {
									store[group][key][agg].Cnt--
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
								store[group][key][agg].Values[j] -= value
								store[group][key][agg].Counters[j]--
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
		store: mStore{
			data: make(map[string]map[string]map[int]*storeEntry),
			groupMap: mapping{
				forward: make(map[string]int64),
				reverse: make(map[int64]string),
			},
			keyMap: mapping{
				forward: make(map[string]int64),
				reverse: make(map[int64]string),
			},
		},
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
