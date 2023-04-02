package cache

import (
	"math/bits"
)

func encode(value *int64, previousValue *int64) (byte, int64) {
	x := *value
	if (x > 63 || x < -64) && previousValue != nil {
		delta := x - *previousValue
		// TODO: discard obvious cases
		if varintSize(delta) < varintSize(x) {
			return encodedFlag, delta
		}
	}
	return regularFlag, x
}

func varintSize(x int64) int {
	if x < 0 {
		x = -x - 1
	}
	n := bits.Len64(uint64(x)) + 1
	if n%7 != 0 {
		return n/7 + 1
	}
	return n / 7
}
