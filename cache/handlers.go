package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

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
