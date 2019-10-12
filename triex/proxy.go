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
	// arbitraryTable table to store arbitrary kvs.
	arbitraryTable table = '\x00'
	// preimageTable the table to store non-trie preimages.
	preimageTable table = '\x01'
	// trieCommitTableIndexKey key to store index of table which tries are committed to.
	trieCommitTableIndexKey = "trie-commit-table-index"

	trieTableA table = '\x02'
	trieTableB table = '\x03'
)

// Trie merkle patricia trie interface.
type Trie interface {
	Get(key []byte) ([]byte, error)
	Update(key, val []byte) error
	Hash() (thor.Bytes32, error)
	Commit() (thor.Bytes32, error)
	NodeIterator(start []byte) (trie.NodeIterator, error)
}

// Proxy to help create tries, which are enhanced by caching, pruning, etc.
type Proxy struct {
	db               trie.Database
	cache            *cache
	preimageGetter   getFunc
	preimagePutter   putFunc
	arbitraryGetter  getFunc
	arbitraryPutter  putFunc
	commitTableIndex byte
}

// New create a trie proxy.
func New(db trie.Database, cacheSizeMB int) *Proxy {
	arbitraryGetter := arbitraryTable.ProxyGetter(db.Get)
	val, _ := arbitraryGetter([]byte(trieCommitTableIndexKey))

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
		arbitraryGetter,
		arbitraryTable.ProxyPutter(db.Put),
		commitTableIndex,
	}
}

// NewTrie create a proxied trie.
func (p *Proxy) NewTrie(root thor.Bytes32, secure bool) Trie {
	nonSecureTrie := p.newTrie(root)
	if secure {
		var (
			hasher    = thor.NewBlake2b()
			keyHasher = func(key []byte) (h thor.Bytes32) {
				hasher.Reset()
				hasher.Write(key)
				hasher.Sum(h[:0])
				return
			}
		)
		return &secureTrie{
			nonSecureTrie,
			keyHasher,
			nil,
			p.preimagePutter,
		}
	}
	return nonSecureTrie
}

// GetPreimage get preimage by given key(hash).
func (p *Proxy) GetPreimage(key []byte) ([]byte, error) {
	return p.preimageGetter(key)
}

// PutPreimage put(save) preimage by give key(hash).
func (p *Proxy) PutPreimage(key, val []byte) error {
	return p.preimagePutter(key, val)
}

// GetArbitrary get arbitrary value by key.
func (p *Proxy) GetArbitrary(key []byte) ([]byte, error) {
	return p.arbitraryGetter(key)
}

// PutArbitrary put arbitrary key value.
func (p *Proxy) PutArbitrary(key, val []byte) error {
	return p.arbitraryPutter(key, val)
}

func (p *Proxy) RollTrieTable() (byte, byte, error) {
	index := p.commitTableIndex + 1
	if err := p.PutArbitrary([]byte(trieCommitTableIndexKey), []byte{index}); err != nil {
		return 0, 0, err
	}
	p.commitTableIndex = index
	t1, t2 := p.trieTables()
	return byte(t1), byte(t2), nil
}

func (p *Proxy) trieTables() (table, table) {
	if p.commitTableIndex%2 == 0 {
		return trieTableA, trieTableB
	}
	return trieTableB, trieTableA
}

func (p *Proxy) newTrie(root thor.Bytes32) *nonSecureTrie {
	t1, t2 := p.trieTables()

	dual := dualTable{t1, t2}

	getter := p.cache.ProxyGetter(dual.ProxyGetter(p.db.Get))
	putter := p.cache.ProxyPutter(dual[0].ProxyPutter(p.db.Put))

	var rawTrie *trie.Trie
	nonSecureTrie := &nonSecureTrie{
		func() (*trie.Trie, error) {
			if rawTrie == nil {
				var err error
				rawTrie, err = trie.New(root, struct {
					getFunc
					hasFunc
					putFunc
				}{
					getter,
					nil,
					putter,
				})
				if err != nil {
					return nil, err
				}
			}
			return rawTrie, nil
		}}
	return nonSecureTrie
}

type nonSecureTrie struct {
	getTrie func() (*trie.Trie, error)
}

func (n *nonSecureTrie) Get(key []byte) ([]byte, error) {
	trie, err := n.getTrie()
	if err != nil {
		return nil, err
	}
	return trie.TryGet(key)
}
func (n *nonSecureTrie) Update(key, val []byte) error {
	trie, err := n.getTrie()
	if err != nil {
		return err
	}
	return trie.TryUpdate(key, val)
}

func (n *nonSecureTrie) Hash() (thor.Bytes32, error) {
	trie, err := n.getTrie()
	if err != nil {
		return thor.Bytes32{}, err
	}
	return trie.Hash(), nil
}

func (n *nonSecureTrie) Commit() (thor.Bytes32, error) {
	trie, err := n.getTrie()
	if err != nil {
		return thor.Bytes32{}, err
	}
	return trie.Commit()
}

func (n *nonSecureTrie) NodeIterator(start []byte) (trie.NodeIterator, error) {
	trie, err := n.getTrie()
	if err != nil {
		return nil, err
	}
	return trie.NodeIterator(start), nil
}

type secureTrie struct {
	*nonSecureTrie
	hasher         func([]byte) thor.Bytes32
	preimages      map[thor.Bytes32][]byte
	preimagePutter putFunc
}

func (s *secureTrie) Get(key []byte) ([]byte, error) {
	return s.nonSecureTrie.Get(s.hasher(key).Bytes())
}

func (s *secureTrie) Update(key, val []byte) error {
	hk := s.hasher(key)
	if err := s.nonSecureTrie.Update(hk[:], val); err != nil {
		return err
	}
	if s.preimages == nil {
		s.preimages = make(map[thor.Bytes32][]byte)
	}
	s.preimages[hk] = val
	return nil
}

func (s *secureTrie) Commit() (thor.Bytes32, error) {
	// for k, v := range s.preimages {
	// 	if err := s.preimagePutter(k[:], v); err != nil {
	// 		return thor.Bytes32{}, err
	// 	}
	// }
	s.preimages = nil
	return s.nonSecureTrie.Commit()
}
