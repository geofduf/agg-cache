package cache

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

func TestRender(t *testing.T) {

	s := new(serializer)
	s.add("I1", &storeEntry{Values: []int64{4, 8}, Counters: []uint32{4, 4}, Cnt: 4})
	s.add("I2", &storeEntry{Values: []int64{0, 0}, Counters: []uint32{0, 0}, Cnt: 0})

	want := `{"status":"ok","data":{"I1":{"values":[4,8],"counters":[4,4]},"I2":{"values":[0,0],"counters":[0,0]}}}`
	have := s.render()

	if !bytes.Equal(have, []byte(want)) {
		t.Fatalf("\nHave: %s\nWant: %s\n", have, want)
	}

}

func TestFixedLengthCapacity(t *testing.T) {

	u32 := uint32(math.MaxUint32)
	i64 := int64(math.MaxInt64)

	s := new(serializer)
	for i := 0; i < 10; i++ {
		s.add("I"+strconv.Itoa(i), &storeEntry{Counters: []uint32{u32, u32}, Values: []int64{i64, i64}, Cnt: u32})
	}

	want := len(s.render()) - s.capacity
	have := fixedLengthCapacity(len(s.rows), len(s.rows[0].e.Counters))

	if have != want {
		t.Fatalf("\nHave: %d\nWant: %d\n", have, want)
	}

}
