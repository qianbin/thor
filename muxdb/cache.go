package muxdb

import (
	"github.com/coocood/freecache"
	"github.com/vechain/thor/kv"
)

type cache struct {
	fc *freecache.Cache
}

func newCache(maxSizeMB int) *cache {
	return &cache{freecache.NewCache(1024 * 1024 * maxSizeMB)}
}

func (c *cache) Evict(key []byte) {
	if c == nil {
		return
	}

	// c.fc.Del(key)

}

func (c *cache) ProxyGet(get getFunc, dontFillCache bool) getFunc {
	if c == nil {
		return get
	}
	return func(key []byte) ([]byte, error) {
		val, err := c.fc.Get(key)
		if err == nil {
			return val, nil
		}

		val, err = get(key)
		if err != nil {
			return nil, err
		}
		if !dontFillCache {
			c.fc.Set(key, val, 24*3600)
		}
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
		c.fc.Set(key, val, 24*3600)
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

		c.fc.Del(key)
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
		c.ProxyGet(getter.Get, false),
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
