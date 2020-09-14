// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"github.com/coocood/freecache"
	lru "github.com/hashicorp/golang-lru"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
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
	dec [trieNodeCacheSeg]*lru.Cache       // for decoded nodes
}

func newTrieCache(encSizeMB int, decCapacity int) *trieCache {
	var cache trieCache
	if encSizeMB > 0 {
		for i := 0; i < trieNodeCacheSeg; i++ {
			cache.enc[i] = freecache.NewCache(encSizeMB * 1024 * 1024 / trieNodeCacheSeg)
		}
	}
	if decCapacity > 0 {
		for i := 0; i < trieNodeCacheSeg; i++ {
			cache.dec[i], _ = lru.New(decCapacity / trieNodeCacheSeg)
		}
	}
	return &cache
}

func (c *trieCache) GetEncoded(name string, key *trie.NodeKey) (val []byte) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}
	if enc := c.enc[i]; enc != nil {
		if key.Scaning {
			val, _ = enc.Peek(thor.Blake2b([]byte(name), key.Hash).Bytes())
		} else {
			val, _ = enc.Get(thor.Blake2b([]byte(name), key.Hash).Bytes())
		}
	}
	return
}
func (c *trieCache) SetEncoded(name string, key *trie.NodeKey, val []byte) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}
	if enc := c.enc[i]; enc != nil {
		_ = enc.Set(thor.Blake2b([]byte(name), key.Hash).Bytes(), val, 8*3600)
	}
}

func (c *trieCache) GetDecoded(name string, key *trie.NodeKey) (val interface{}) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}

	if dec := c.dec[i]; dec != nil {
		if key.Scaning {
			val, _ = dec.Peek(string(thor.Blake2b([]byte(name), key.Hash).Bytes()))
		} else {
			val, _ = dec.Get(string(thor.Blake2b([]byte(name), key.Hash).Bytes()))
		}
	}
	return
}

func (c *trieCache) SetDecoded(name string, key *trie.NodeKey, val interface{}) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}

	if dec := c.dec[i]; dec != nil {
		dec.Add(string(thor.Blake2b([]byte(name), key.Hash).Bytes()), val)
	}
}
