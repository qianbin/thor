package muxdb

import (
	"fmt"
	"math"
	"time"

	"github.com/coocood/freecache"
	lru "github.com/hashicorp/golang-lru"
)

type trieCache struct {
	enc [8]*freecache.Cache
	dec *lru.Cache
}

func newTrieCache(encSizeMB int, decCount int) *trieCache {
	c := &trieCache{}
	if encSizeMB > 0 {
		const factor = 1.5
		var sum float64
		for i := 0; i < len(c.enc); i++ {
			sum += math.Pow(factor, float64(i))
		}

		for i := 0; i < len(c.enc); i++ {
			size := float64(encSizeMB*1024*1024) * math.Pow(factor, float64(i)) / sum
			c.enc[i] = freecache.NewCache(int(size))
		}

		go func() {
			for {
				time.Sleep(time.Minute)
				for i, e := range c.enc {
					fmt.Println(i, "[", e.EntryCount(), e.HitCount(), e.MissCount(), e.EvacuateCount(), e.HitRate(), "]")
				}
			}
		}()
	}

	if decCount > 0 {
		c.dec, _ = lru.New(decCount)
	}

	return c
}

func (c *trieCache) GetEncoded(key []byte, pathLen int, peek bool) []byte {
	i := pathLen

	if l := len(c.enc) - 1; i > l {
		i = l
	}

	e := c.enc[i]

	if e == nil {
		return nil
	}

	if peek {
		val, err := e.Peek(key)
		if err != nil {
			return nil
		}
		return val
	}
	val, err := e.Get(key)
	if err != nil {
		return nil
	}
	return val
}
func (c *trieCache) SetEncoded(key, val []byte, pathLen int) {
	i := pathLen

	if l := len(c.enc) - 1; i > l {
		i = l
	}

	e := c.enc[i]

	if e == nil {
		return
	}
	e.Set(key, val, 8*3600)
}

func (c *trieCache) GetDecoded(key []byte, peek bool) interface{} {
	if c.dec == nil {
		return nil
	}

	if peek {
		val, _ := c.dec.Peek(string(key))
		return val
	}

	val, _ := c.dec.Get(string(key))
	return val
}

func (c *trieCache) SetDecoded(key []byte, val interface{}) {
	if c.dec == nil {
		return
	}
	c.dec.Add(string(key), val)
}
