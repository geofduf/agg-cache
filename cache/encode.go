package cache

import (
	"math/bits"
)

func encode(value *int64, previousValue *int64) (byte, int64) {
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
