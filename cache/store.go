package cache

import (
	"fmt"
	"sync"
)

type storeEntry struct {
	Values   []int64  `json:"values"`
	Counters []uint32 `json:"counters"`
	Cnt      uint32   `json:"-"`
}

type groupInfo struct {
	name     string
	forward  map[string]int
	reverse  []string
	recycler map[int]struct{}
}

type statistics struct {
	groups      int
	keys        int
	emptyGroups int
	emptyKeys   int
}

type store struct {
	sync.RWMutex
	data       [][][]*storeEntry
	forward    map[string]int
	reverse    []*groupInfo
	recycler   map[int]struct{}
	statistics statistics
}

func (s *store) getIdentifiers(group string, key string, length int, numberOfValues int) (int, int) {

	if gid, found := s.forward[group]; !found {

		g := &groupInfo{
			name:     group,
			forward:  map[string]int{key: 0},
			reverse:  []string{key},
			recycler: make(map[int]struct{}),
		}

		if len(s.recycler) > 0 {

			for id := range s.recycler {
				gid = id
				delete(s.recycler, id)
				break
			}

			s.reverse[gid] = g
			s.data[gid] = [][]*storeEntry{make([]*storeEntry, length)}

			s.statistics.emptyGroups -= 1

			logger.Debug("STR", fmt.Sprintf("recycling group %d", gid))
			logger.Debug("STR", fmt.Sprintf("creating key 0 for group %d (%d)", gid, len(g.recycler)))

		} else {

			gid = len(s.reverse)

			s.reverse = append(s.reverse, g)
			s.data = append(s.data, [][]*storeEntry{make([]*storeEntry, length)})

			logger.Debug("STR", fmt.Sprintf("creating group %d", gid))
			logger.Debug("STR", fmt.Sprintf("creating key 0 for group %d (%d)", gid, len(g.recycler)))

		}

		for i := 0; i < length; i++ {
			s.data[gid][0][i] = &storeEntry{
				Values:   make([]int64, numberOfValues),
				Counters: make([]uint32, numberOfValues),
			}
		}

		s.forward[group] = gid

		s.statistics.groups += 1
		s.statistics.keys += 1

		return gid, 0

	} else if kid, found := s.reverse[gid].forward[key]; !found {

		g := s.reverse[gid]

		if len(g.recycler) > 0 {

			for id := range g.recycler {
				kid = id
				delete(g.recycler, id)
				break
			}

			g.reverse[kid] = key
			s.data[gid][kid] = make([]*storeEntry, length)

			s.statistics.emptyKeys -= 1

			logger.Debug("STR", fmt.Sprintf("recycling key %d for group %d (%d)", kid, gid, len(g.recycler)))

		} else {

			kid = len(g.reverse)

			g.reverse = append(g.reverse, key)
			s.data[gid] = append(s.data[gid], make([]*storeEntry, length))

			logger.Debug("STR", fmt.Sprintf("creating key %d for group %d (%d)", kid, gid, len(g.recycler)))

		}

		for i := 0; i < length; i++ {
			s.data[gid][kid][i] = &storeEntry{
				Values:   make([]int64, numberOfValues),
				Counters: make([]uint32, numberOfValues),
			}
		}

		g.forward[key] = kid

		s.statistics.keys += 1

		return gid, kid

	} else {

		return gid, kid

	}

}

func (s *store) releaseKey(gid int, kid int) {

	if len(s.reverse[gid].forward) == 1 {

		delete(s.forward, s.reverse[gid].name)
		s.reverse[gid] = nil
		s.data[gid] = nil
		s.recycler[gid] = struct{}{}

		s.statistics.groups -= 1
		s.statistics.keys -= 1
		s.statistics.emptyGroups += 1

		logger.Debug("STR", fmt.Sprintf("releasing group %d", gid))

	} else {

		delete(s.reverse[gid].forward, s.reverse[gid].reverse[kid])
		s.data[gid][kid] = nil
		s.reverse[gid].recycler[kid] = struct{}{}

		s.statistics.keys -= 1
		s.statistics.emptyKeys += 1

		logger.Debug("STR", fmt.Sprintf("releasing key %d for group %d (%d)", kid, gid, len(s.reverse[gid].recycler)))

	}

}
