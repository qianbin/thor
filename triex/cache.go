// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package triex

import (
	"time"

	"github.com/allegro/bigcache"
)

type cache struct {
	bigcache *bigcache.BigCache
}

func newCache(maxSizeMB int) *cache {
	bigcache, _ := bigcache.NewBigCache(bigcache.Config{
		Shards:             1024,
		LifeWindow:         time.Hour * 10,
		MaxEntriesInWindow: maxSizeMB * 1024,
		MaxEntrySize:       512,
		HardMaxCacheSize:   maxSizeMB,
	})
	return &cache{bigcache}
}

func (c *cache) ProxyGetter(get getFunc) getFunc {
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

func (c *cache) ProxyPutter(put putFunc) putFunc {
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
