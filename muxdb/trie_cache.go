// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"encoding/binary"

	"github.com/coocood/freecache"
	lru "github.com/hashicorp/golang-lru"
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

func (c *trieCache) GetEncoded(key *trie.NodeKey) (val []byte) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}
	if enc := c.enc[i]; enc != nil {
		var buf [36]byte
		binary.BigEndian.PutUint32(buf[:], key.Revision)
		copy(buf[4:], key.Hash)
		if key.Scaning {
			val, _ = enc.Peek(buf[:])
		} else {
			val, _ = enc.Get(buf[:])
		}
	}
	return
}
func (c *trieCache) SetEncoded(key *trie.NodeKey, val []byte) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}
	if enc := c.enc[i]; enc != nil {
		var buf [36]byte
		binary.BigEndian.PutUint32(buf[:], key.Revision)
		copy(buf[4:], key.Hash)
		_ = enc.Set(buf[:], val, 8*3600)
	}
}

func (c *trieCache) GetDecoded(key *trie.NodeKey) (val interface{}) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}

	if dec := c.dec[i]; dec != nil {
		var buf [36]byte
		binary.BigEndian.PutUint32(buf[:], key.Revision)
		copy(buf[4:], key.Hash)
		if key.Scaning {
			val, _ = dec.Peek(string(buf[:]))
		} else {
			val, _ = dec.Get(string(buf[:]))
		}
	}
	return
}

func (c *trieCache) SetDecoded(key *trie.NodeKey, val interface{}) {
	i := len(key.Path)
	if i >= trieNodeCacheSeg {
		i = trieNodeCacheSeg - 1
	}

	if dec := c.dec[i]; dec != nil {
		var buf [36]byte
		binary.BigEndian.PutUint32(buf[:], key.Revision)
		copy(buf[4:], key.Hash)
		dec.Add(string(buf[:]), val)
	}
}
