package muxdb

import (
	"time"

	"github.com/allegro/bigcache"
	"github.com/vechain/thor/kv"
)

type cache struct {
	bigcache *bigcache.BigCache
}

func newCache(maxSizeMB int) *cache {
	bigcache, _ := bigcache.NewBigCache(bigcache.Config{
		Shards:             1024,
		LifeWindow:         time.Hour * 24 * 7,
		MaxEntriesInWindow: maxSizeMB * 1024,
		MaxEntrySize:       512,
		HardMaxCacheSize:   maxSizeMB,
	})
	return &cache{bigcache}
}

func (c *cache) ProxyGet(get getFunc) getFunc {
	if c == nil {
		return get
	}
	return func(key []byte) ([]byte, error) {
		val, err := c.bigcache.Get(string(key))
		if err == nil {
			return val, nil
		}

		val, err = get(key)
		if err != nil {
			return nil, err
		}

		c.bigcache.Set(string(key), val)
		return val, nil
	}
}

func (c *cache) ProxyPut(put putFunc) putFunc {
	if c == nil {
		return put
	}
	return func(key, val []byte) error {
		if err := put(key, val); err != nil {
			return err
		}
		c.bigcache.Set(string(key), val)
		return nil
	}
}

func (c *cache) ProxyDelete(del deleteFunc) deleteFunc {
	if c == nil {
		return del
	}
	return func(key []byte) error {
		if err := del(key); err != nil {
			return err
		}
		c.bigcache.Delete(string(key))
		return nil
	}
}

func (c *cache) ProxyGetter(getter kv.Getter) kv.Getter {
	if c == nil {
		return getter
	}
	return struct {
		getFunc
		hasFunc
	}{
		c.ProxyGet(getter.Get),
		getter.Has,
	}
}

func (c *cache) ProxyPutter(putter kv.Putter) kv.Putter {
	if c == nil {
		return putter
	}

	return struct {
		putFunc
		deleteFunc
	}{
		c.ProxyPut(putter.Put),
		c.ProxyDelete(putter.Delete),
	}
}
