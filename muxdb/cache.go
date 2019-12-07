package muxdb

import (
	"fmt"
	"time"

	"github.com/coocood/freecache"
	lru "github.com/hashicorp/golang-lru"
)

const trieEncCacheSlice = 8

type trieCache struct {
	enc [trieEncCacheSlice]*freecache.Cache // for encoded nodes
	dec *lru.Cache                          // for decoded nodes
}

func newTrieCache(encSizeMB int, decCount int) *trieCache {
	var cache trieCache
	if encSizeMB > 0 {
		for i := 0; i < trieEncCacheSlice; i++ {
			cache.enc[i] = freecache.NewCache(encSizeMB * 1024 * 1024 / trieEncCacheSlice)
		}
		go func() {
			for {
				time.Sleep(time.Minute)
				for i, e := range cache.enc {
					fmt.Println(i, "[", e.EntryCount(), e.HitCount(), e.MissCount(), e.EvacuateCount(), e.HitRate(), "]")
				}
			}
		}()
	}

	if decCount > 0 {
		cache.dec, _ = lru.New(decCount)
	}
	return &cache
}

func (c *trieCache) GetEncoded(key []byte, pathLen int, peek bool) (val []byte) {
	enc := c.enc[pathLen%trieEncCacheSlice]
	if enc == nil {
		return nil
	}

	if peek {
		val, _ = enc.Peek(key)
	} else {
		val, _ = enc.Get(key)
	}
	return val
}
func (c *trieCache) SetEncoded(key, val []byte, pathLen int) {
	enc := c.enc[pathLen%trieEncCacheSlice]
	if enc == nil {
		return
	}
	enc.Set(key, val, 8*3600)
}

func (c *trieCache) GetDecoded(key []byte, peek bool) (val interface{}) {
	if c.dec == nil {
		return nil
	}
	if peek {
		val, _ = c.dec.Peek(string(key))
	} else {
		val, _ = c.dec.Get(string(key))
	}
	return val
}

func (c *trieCache) SetDecoded(key []byte, val interface{}) {
	if c.dec == nil {
		return
	}
	c.dec.Add(string(key), val)
}
