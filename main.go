package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/geofduf/agg-cache/cache"
	"github.com/geofduf/logging"
)

type levels []int

func (l levels) String() string {
	var s []string
	for _, v := range l {
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
	return nil
}

func main() {

	var frequency, logLevel int
	var listen string

	aggs := levels{300, 1800, 3600, 7200, 14400, 86400}

	flag.StringVar(&listen, "listen", "127.0.0.1:8888", "HTTP listening address:port")
	flag.IntVar(&logLevel, "logging", 3, "Logging level (1-5)")
	flag.IntVar(&frequency, "frequency", 60, "Data processing frequency in seconds")
	flag.Var(&aggs, "levels", "Comma-separated list of aggregation levels in seconds")
	flag.Parse()

	logger := logging.GetLogger()
	logger.SetLevel(logLevel)

	c, err := cache.NewCache(frequency, aggs)
	if err != nil {
		logger.Fatal("INI", err.Error())
	}

	http.HandleFunc("/group/", c.GroupHandler)
	http.HandleFunc("/insert", c.InsertHandler)

	if err := http.ListenAndServe(listen, nil); err != nil {
		logger.Fatal("INI", err.Error())
	}

}
