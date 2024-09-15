package multibuffer

import "math"

func BestRoot(available int32, size int32) int32 {
	avail := available - 2 // reserve a couple
	if avail <= 1 {
		return 1
	}
	k := int32(math.MaxInt32)
	i := 1.0
	for k > avail {
		i++
		k = int32(math.Ceil(math.Pow(float64(size), 1.0/i)))
	}
	return k
}

func BestFactor(available int32, size int32) int32 {
	avail := available - 2 // reserve a couple
	if avail <= 1 {
		return 1
	}
	k := size
	i := 1.0
	for k > avail {
		i++
		k = int32(math.Ceil(float64(size) / i))
	}
	return k
}
