package muxdb

import (
	"github.com/coocood/freecache"
	lru "github.com/hashicorp/golang-lru"
)

type trieCache struct {
	enc *freecache.Cache // for encoded nodes
	dec *lru.ARCCache    // for decoded nodes
}

func newTrieCache(encSizeMB int, decCount int) *trieCache {
	if encSizeMB > 0 && decCount > 0 {
		dec, _ := lru.NewARC(decCount)
		return &trieCache{
			freecache.NewCache(encSizeMB * 1024 * 1024),
			dec,
		}
	}
	return nil
}

func (c *trieCache) GetEncoded(key []byte) []byte {
	if c == nil {
		return nil
	}
	val, err := c.enc.Get(key)
	if err != nil {
		return nil
	}
	return val
}
func (c *trieCache) SetEncoded(key, val []byte) {
	if c == nil {
		return
	}
	c.enc.Set(key, val, 8*3600)
}

func (c *trieCache) GetDecoded(key []byte) interface{} {
	if c == nil {
		return nil
	}

	val, _ := c.dec.Get(string(key))
	return val
}

func (c *trieCache) SetDecoded(key []byte, val interface{}) {
	if c == nil {
		return
	}
	c.dec.Add(string(key), val)
}
