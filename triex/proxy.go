// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

// Package triex implements trie proxy, which nonintrusively enhances trie performance.
package triex

import (
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

const (
	// preimageTable the table to store non-trie preimages.
	preimageTable table = "\x00"
	// trieCommitTableIndexKey key to store index of table which tries are committed to.
	trieCommitTableIndexKey = "trie-commit-table-index"
)

// trieTables tables to store tries.
var trieTables = dualTable{"\x01", "\x02"}

// Trie merkle patricia trie interface.
type Trie interface {
	Get(key []byte) ([]byte, error)
	Update(key, val []byte) error
	Hash() thor.Bytes32
	Commit() (thor.Bytes32, error)
	NodeIterator(start []byte) trie.NodeIterator
}

// Proxy to help create tries, which are enhanced by caching, pruning, etc.
type Proxy struct {
	db               trie.Database
	cache            *cache
	preimageGetter   getFunc
	preimagePutter   putFunc
	commitTableIndex byte
}

// New create a trie proxy.
func New(db trie.Database, cacheSizeMB int) *Proxy {
	val, _ := db.Get([]byte(trieCommitTableIndexKey))

	commitTableIndex := byte(0)
	if len(val) > 0 {
		commitTableIndex = val[0]
	}
	var cache *cache
	if cacheSizeMB > 0 {
		cache = newCache(cacheSizeMB)
	}
	return &Proxy{
		db,
		cache,
		cache.ProxyGetter(preimageTable.ProxyGetter(db.Get)),
		cache.ProxyPutter(preimageTable.ProxyPutter(db.Put)),
		commitTableIndex,
	}
}

// NewTrie create a proxied trie.
func (p *Proxy) NewTrie(root thor.Bytes32, secure bool) Trie {
	if secure {
		var (
			hasher = thor.NewBlake2b()
		)
		keyHasher := func(key []byte) (h thor.Bytes32) {
			hasher.Reset()
			hasher.Write(key)
			hasher.Sum(h[:0])
			return
		}
		return &secureTrie{
			p.newTrie(root),
			keyHasher,
			make(map[thor.Bytes32][]byte),
			p.preimagePutter,
		}
	}
	return &nonSecureTrie{p.newTrie(root)}
}

// GetPreimage get preimage by given key(hash).
func (p *Proxy) GetPreimage(key []byte) ([]byte, error) {
	return p.preimageGetter(key)
}

// PutPreimage put(save) preimage by give key(hash).
func (p *Proxy) PutPreimage(key, val []byte) error {
	return p.preimagePutter(key, val)
}

func (p *Proxy) newTrie(root thor.Bytes32) *trie.Trie {
	commitTable := trieTables[p.commitTableIndex%2]
	// here skip error check is safe
	tr, _ := trie.New(root, struct {
		getFunc
		hasFunc
		putFunc
	}{
		p.cache.ProxyGetter(trieTables.ProxyGetter(p.db.Get)),
		nil,
		p.cache.ProxyPutter(commitTable.ProxyPutter(p.db.Put)),
	})
	return tr
}

type nonSecureTrie struct {
	*trie.Trie
}

func (n *nonSecureTrie) Get(key []byte) ([]byte, error) {
	return n.Trie.TryGet(key)
}
func (n *nonSecureTrie) Update(key, val []byte) error {
	return n.Trie.TryUpdate(key, val)
}

type secureTrie struct {
	*trie.Trie
	hasher         func([]byte) thor.Bytes32
	preimages      map[thor.Bytes32][]byte
	preimagePutter putFunc
}

func (s *secureTrie) Get(key []byte) ([]byte, error) {
	return s.Trie.TryGet(s.hasher(key).Bytes())
}

func (s *secureTrie) Update(key, val []byte) error {
	hk := s.hasher(key)
	s.preimages[hk] = append([]byte(nil), val...)
	return s.Trie.TryUpdate(hk[:], val)
}

func (s *secureTrie) Commit() (thor.Bytes32, error) {
	for k, v := range s.preimages {
		if err := s.preimagePutter(k[:], v); err != nil {
			return thor.Bytes32{}, err
		}
	}
	return s.Trie.Commit()
}
