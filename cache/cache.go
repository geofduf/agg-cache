package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/geofduf/logging"
)

const (
	missingFlag byte = iota
	regularFlag
	encodedFlag
	entryFlag
)

var logger *logging.Logger

type input struct {
	Group  string   `json:"group"`
	Key    string   `json:"key"`
	Values []*int64 `json:"values"`
}

type queue struct {
	sync.Mutex
	data map[int][]input
}

type cache struct {
	store     store
	queue     queue
	aggs      []int
	frequency int
}

func (c *cache) InsertHandler(w http.ResponseWriter, req *http.Request) {

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
		ts := int(time.Now().Unix()) / c.frequency * c.frequency
		c.queue.Lock()
		if _, ok := c.queue.data[ts]; ok {
			c.queue.data[ts] = append(c.queue.data[ts], entries...)
		} else {
			c.queue.data[ts] = entries
		}
		c.queue.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status": "%s", "message": "%s"}`, status, message)

}

func (c *cache) GroupHandler(w http.ResponseWriter, req *http.Request) {

	if req.Method != http.MethodGet {
		w.Header().Set("Allow", "Get")
		http.Error(w, "", 405)
		return
	}

	valid := false
	agg, err := strconv.Atoi(req.FormValue("agg"))
	if err == nil {
		for i, v := range c.aggs {
			if agg == v {
				valid = true
				agg = i
				break
			}
		}
	}

	if !valid {
		http.Error(w, "", 400)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	group := req.FormValue("group")

	c.store.RLock()
	if groupId, ok := c.store.forward[group]; ok {
		s := &serializer{rows: make([]row, 0, len(c.store.data[groupId]))}
		for keyId, v := range c.store.data[groupId] {
			if v != nil {
				s.add(c.store.reverse[groupId].reverse[keyId], v[agg])
			}
		}
		w.Write(s.render())
	} else {
		io.WriteString(w, responseEmpty)
	}
	c.store.RUnlock()

}

func (c *cache) processData() {

	aggs := c.aggs
	aggsLength := len(aggs)
	rawData := make(map[int][]byte)
	container := make([]byte, binary.MaxVarintLen64)
	var buf bytes.Buffer

	index := make(map[int]map[int]bool)
	for _, agg := range aggs {
		index[agg] = make(map[int]bool)
	}

	napDuration := c.frequency - int(time.Now().Unix())%c.frequency
	logger.System("PRO", fmt.Sprintf("scheduler will start in %d second(s)", napDuration))
	time.Sleep(time.Duration(napDuration) * time.Second)

	ticker := time.NewTicker(time.Duration(c.frequency) * time.Second)

	for {
		t := <-ticker.C
		start := time.Now()

		if int(start.Unix())%c.frequency > 3 {
			logger.Warning("PRO", "probably drifting")
		}

		c.queue.Lock()
		queue := c.queue.data
		c.queue.data = make(map[int][]input)
		c.queue.Unlock()

		buckets := make([]int, 0, len(queue))
		for k := range queue {
			buckets = append(buckets, k)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(buckets)))

		c.store.Lock()

		if elapsed := time.Since(start); elapsed > 1000000000 {
			logger.Warning("PRO", fmt.Sprintf("offset of %d seconds", elapsed/1000000000))
		}

		ts := int(time.Now().Unix()) / c.frequency * c.frequency

		for _, bucket := range buckets {
			for aggIndex := range aggs {
				if bucket >= ts-aggs[aggIndex] {
					buf.Reset()
					for _, v := range queue[bucket] {
						previousValue := new(int64)
						groupId, keyId := c.store.getIdentifiers(v.Group, v.Key, aggsLength, len(v.Values))
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
									c.store.data[groupId][keyId][j].Values[i] += *v.Values[i]
									c.store.data[groupId][keyId][j].Counters[i]++
								}
							}
						}
						for i := aggIndex; i < aggsLength; i++ {
							c.store.data[groupId][keyId][i].Cnt++
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
								if i == aggsLength-1 && c.store.data[groupId][keyId][i].Cnt == 1 {
									c.store.releaseKey(groupId, keyId)
									skip = true
								} else {
									c.store.data[groupId][keyId][i].Cnt--
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
								c.store.data[groupId][keyId][i].Values[j] -= value
								c.store.data[groupId][keyId][i].Counters[j]--
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
		c.store.Unlock()
		logger.Debug("PRO", fmt.Sprintf("ticker: %s duration: %s store: %v", t, time.Since(start), c.store.statistics))
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

func New(frequency int, aggs []int) (*cache, error) {
	sort.Ints(aggs)
	if frequency <= 0 {
		return nil, errors.New("frequency must be a positive integer")
	}
	if frequency > aggs[0] {
		return nil, errors.New("frequency must be lower or equal to the first aggregation level")
	}
	c := &cache{
		queue:     queue{data: make(map[int][]input)},
		store:     store{forward: make(map[string]int), recycler: make(map[int]struct{})},
		aggs:      aggs,
		frequency: frequency,
	}
	go c.processData()
	return c, nil
}

func init() {
	logger = logging.GetLogger()
}
