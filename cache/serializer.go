package cache

// Just having some fun with benchmarks. Should switch back to encoding/json for readability.

import (
	"strconv"
)

const (
	serializerBasePrefix     = `{"status":"ok","data":{`
	serializerRowPrefix      = '"'
	serializerValuesPrefix   = `":{"values":[`
	serializerCountersPrefix = `],"counters":[`
	serializerRowSuffix      = "]}"
	serializerBaseSuffix     = "}}"
	serializerComma          = ','
	responseEmpty            = serializerBasePrefix + serializerBaseSuffix
	responseError            = `{"status":"error","data":{}}`
)

type row struct {
	k string
	e *storeEntry
}

type serializer struct {
	rows     []row
	capacity int
}

func (s *serializer) add(k string, e *storeEntry) {
	s.rows = append(s.rows, row{k: k, e: e})
	s.capacity += len(k)
}

func (s *serializer) render() []byte {

	r := len(s.rows)
	if r == 0 {
		return []byte(responseEmpty)
	}

	c := len(s.rows[0].e.Counters)
	if c == 0 {
		return []byte(responseEmpty)
	}

	buf := append(make([]byte, 0, s.capacity+fixedLengthCapacity(r, c)), serializerBasePrefix...)

	first := true
	for _, v := range s.rows {
		if !first {
			buf = append(buf, serializerComma)
		}
		buf = append(buf, serializerRowPrefix)
		buf = append(buf, v.k...)
		buf = append(buf, serializerValuesPrefix...)
		buf = strconv.AppendInt(buf, v.e.Values[0], 10)
		for i := 1; i < c; i++ {
			buf = append(buf, serializerComma)
			buf = strconv.AppendInt(buf, v.e.Values[i], 10)
		}
		buf = append(buf, serializerCountersPrefix...)
		buf = strconv.AppendInt(buf, int64(v.e.Counters[0]), 10)
		for i := 1; i < c; i++ {
			buf = append(buf, serializerComma)
			buf = strconv.AppendInt(buf, int64(v.e.Counters[i]), 10)
		}
		buf = append(buf, serializerRowSuffix...)
		first = false
	}

	return append(buf, serializerBaseSuffix...)

}

func fixedLengthCapacity(rows, cols int) int {
	return 24 + rows*(31+cols*29+(cols-1)*2)
}
