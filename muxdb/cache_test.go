package muxdb

import (
	"fmt"
	"math"
	"testing"
)

func TestTrieCache(t *testing.T) {
	var sum float64
	for i := 0; i < 8; i++ {
		sum += math.Pow(1.5, float64(i))
	}
	for i := 0; i < 8; i++ {
		fmt.Println(4096 * math.Pow(1.5, float64(i)) / sum)
	}

}

// 32.25196850393701
// 64.50393700787401
// 129.00787401574803
// 258.01574803149606
// 516.0314960629921
// 1032.0629921259842
// 2064.1259842519685
