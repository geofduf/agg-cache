package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/geofduf/logging"
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
			return errors.New(fmt.Sprintf(`"%s" is not a positive integer`, level))
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
	Group  string `json:"group"`
	Key    string `json:"key"`
	Values []*int `json:"values"`
}

type output struct {
	Values   []*int `json:"values"`
	Counters []int  `json:"counters"`
}

type response struct {
	Status string            `json:"status"`
	Data   map[string]output `json:"data"`
}

type storeEntry struct {
	values   []int
	counters []int
	cnt      int
}

type mStore struct {
	sync.RWMutex
	data map[string]map[string]map[int]*storeEntry
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
		logger.WARNING("INS", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
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
	io.WriteString(w, fmt.Sprintf(`{"status": "%s", "message": "%s"}`, status, message))

}

func (app *application) groupHandler(w http.ResponseWriter, req *http.Request) {

	var (
		group string
		agg   int
		err   error
	)

	param := req.FormValue("aggregation")
	slugs := strings.Split(req.URL.Path, "/")
	valid := false
	message := "invalid request"

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
			Data:   make(map[string]output),
		}
		if _, ok := app.store.data[group]; ok {
			for k := range app.store.data[group] {
				length := len(app.store.data[group][k][agg].counters)
				r.Data[k] = output{
					Values:   make([]*int, length),
					Counters: make([]int, length),
				}
				copy(r.Data[k].Counters, app.store.data[group][k][agg].counters)
				for i := 0; i < length; i++ {
					if r.Data[k].Counters[i] > 0 {
						r.Data[k].Values[i] = new(int)
						*r.Data[k].Values[i] = app.store.data[group][k][agg].values[i]
					}
				}
			}
		}
		if body, err := json.Marshal(r); err != nil {
			message = "cannot build response"
		} else {
			w.Write(body)
			return
		}
	}

	logger.WARNING("GET", fmt.Sprintf("%s (%s)", message, req.RemoteAddr))
	io.WriteString(w, `{"status": "error", "data": {}}`)

}

func (app *application) processData() {

	aggs := config.aggs
	aggsLength := len(aggs)
	rawData := make(map[int][]input)

	index := make(map[int]map[int]bool)
	for _, agg := range aggs {
		index[agg] = make(map[int]bool)
	}

	napDuration := config.frequency - int(time.Now().Unix())%config.frequency
	logger.SYSTEM("PRO", fmt.Sprintf("scheduler will start in %d second(s)", napDuration))
	time.Sleep(time.Duration(napDuration) * time.Second)

	ticker := time.NewTicker(time.Duration(config.frequency) * time.Second)

	for {
		t := <-ticker.C
		start := time.Now()

		if int(start.Unix())%config.frequency > 3 {
			logger.WARNING("PRO", "probably drifting")
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
			logger.WARNING("PRO", fmt.Sprintf("offset of %d seconds", elapsed/1000000000))
		}

		ts := int(time.Now().Unix()) / config.frequency * config.frequency

		for _, bucket := range buckets {
			if _, ok := rawData[bucket]; ok {
				rawData[bucket] = append(rawData[bucket], queue[bucket]...)
			} else {
				rawData[bucket] = queue[bucket]
			}
			for aggIndex := range aggs {
				if bucket >= ts-aggs[aggIndex] {
					for _, v := range queue[bucket] {
						if _, ok := store[v.Group]; !ok {
							store[v.Group] = make(map[string]map[int]*storeEntry)
							store[v.Group][v.Key] = make(map[int]*storeEntry)
							for i := 0; i < aggsLength; i++ {
								store[v.Group][v.Key][aggs[i]] = &storeEntry{
									values:   make([]int, len(v.Values)),
									counters: make([]int, len(v.Values)),
								}
							}
						} else if _, ok := store[v.Group][v.Key]; !ok {
							store[v.Group][v.Key] = make(map[int]*storeEntry)
							for i := 0; i < aggsLength; i++ {
								store[v.Group][v.Key][aggs[i]] = &storeEntry{
									values:   make([]int, len(v.Values)),
									counters: make([]int, len(v.Values)),
								}
							}
						}
						for i := aggIndex; i < aggsLength; i++ {
							for j := range v.Values {
								if v.Values[j] != nil {
									store[v.Group][v.Key][aggs[i]].values[j] += *v.Values[j]
									store[v.Group][v.Key][aggs[i]].counters[j]++
								}
							}
							store[v.Group][v.Key][aggs[i]].cnt++
						}
					}
					for i := aggIndex; i < aggsLength; i++ {
						index[aggs[i]][bucket] = true
					}
					break
				}
			}
		}
		for i, agg := range aggs {
			for bucket := range index[agg] {
				if bucket < ts-agg {
					for _, v := range rawData[bucket] {
						if i == aggsLength-1 && store[v.Group][v.Key][agg].cnt == 1 {
							delete(store[v.Group], v.Key)
							if len(store[v.Group]) == 0 {
								delete(store, v.Group)
							}
						} else {
							for j := range v.Values {
								if v.Values[j] != nil {
									store[v.Group][v.Key][agg].values[j] -= *v.Values[j]
									store[v.Group][v.Key][agg].counters[j]--
								}
							}
							store[v.Group][v.Key][agg].cnt--
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
		logger.DEBUG("PRO", fmt.Sprintf("ticker: %s duration: %s", t, time.Since(start)))
	}
}

func main() {
	flag.Parse()
	if config.frequency <= 0 {
		logger.FATAL("INI", "frequency must be a positive integer")
	} else if config.frequency > config.aggs[0] {
		logger.FATAL("INI", "frequency must be lower or equal to the first aggregation level")
	}
	logger.SetLevel(config.logging)
	app := application{
		queue: mQueue{data: make(map[int][]input)},
		store: mStore{data: make(map[string]map[string]map[int]*storeEntry)},
	}
	go app.processData()
	http.HandleFunc("/group/", app.groupHandler)
	http.HandleFunc("/insert", app.insertHandler)
	if err := http.ListenAndServe(config.listen, nil); err != nil {
		logger.FATAL("INI", err.Error())
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
