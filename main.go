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
	return nil
}

type config struct {
	aggs      levels
	frequency int
	logging   int
	listen    string
}

func main() {

	cfg := config{aggs: levels{300, 1800, 3600, 7200, 14400, 86400}}

	flag.StringVar(&cfg.listen, "listen", "127.0.0.1:8888", "HTTP listening address:port")
	flag.IntVar(&cfg.logging, "logging", 3, "Logging level")
	flag.IntVar(&cfg.frequency, "frequency", 60, "Data processing frequency in seconds")
	flag.Var(&cfg.aggs, "levels", "Comma-separated list of aggregation levels in seconds")
	flag.Parse()

	logger := logging.GetLogger()
	logger.SetLevel(cfg.logging)

	c, err := cache.NewCache(cfg.frequency, cfg.aggs)
	if err != nil {
		logger.Fatal("INI", err.Error())
	}

	http.HandleFunc("/group/", c.GroupHandler)
	http.HandleFunc("/insert", c.InsertHandler)

	if err := http.ListenAndServe(cfg.listen, nil); err != nil {
		logger.Fatal("INI", err.Error())
	}

}
