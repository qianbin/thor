// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"github.com/coocood/freecache"
)

const (
	// trieNodeCacheSeg number of segments for the trie node cache.
	//
	// In practical, it's more efficient if divide the cache into
	// several segments by node path length.
	// 8 is the ideal value now.
	trieNodeCacheSeg = 8
)

type trieCache struct {
	enc [trieNodeCacheSeg]*freecache.Cache // for encoded nodes
}

func newTrieCache(encSizeMB int) *trieCache {
	var cache trieCache
	if encSizeMB > 0 {
		for i := 0; i < trieNodeCacheSeg; i++ {
			cache.enc[i] = freecache.NewCache(encSizeMB * 1024 * 1024 / trieNodeCacheSeg)
		}
	}
	return &cache
}

func (c *trieCache) Get(key []byte, pathLen int, peek bool) (val []byte) {
	i := pathLen
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}
	if enc := c.enc[i]; enc != nil {
		if peek {
			val, _ = enc.Peek(key)
		} else {
			val, _ = enc.Get(key)
		}
	}
	return
}
func (c *trieCache) Set(key, val []byte, pathLen int) {
	i := pathLen
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}
	if enc := c.enc[i]; enc != nil {
		_ = enc.Set(key, val, 8*3600)
	}
}
