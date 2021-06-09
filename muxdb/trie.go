// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

// individual functions of trie.DatabaseXXXEx interface.
type (
	getExFunc     func(key *trie.CompositKey) (trie.CompositValue, error)
	putExFunc     func(key *trie.CompositKey, val trie.CompositValue) error
	directGetFunc func(key []byte, nodeVer uint32) ([]byte, []byte, bool, error)
)

func (f getExFunc) GetEx(key *trie.CompositKey) (trie.CompositValue, error) { return f(key) }
func (f putExFunc) PutEx(key *trie.CompositKey, val trie.CompositValue) error {
	return f(key, val)
}

func (f directGetFunc) DirectGet(key []byte, nodeVer uint32) ([]byte, []byte, bool, error) {
	return f(key, nodeVer)
}

var (
	_ trie.DatabaseReaderEx = getExFunc(nil)
	_ trie.DatabaseWriterEx = putExFunc(nil)
	_ trie.DirectReader     = directGetFunc(nil)
)

type TrieJournalItem struct {
	K, V, Extra []byte
}

// Trie is the managed trie.
type Trie struct {
	store        kv.Store
	name         string
	originalRoot thor.Bytes32
	cache        *trieCache
	keyBuf       trieNodeKeyBuf
	secure       bool
	lazyInit     func() (*trie.Trie, error)
	secureKeys   map[thor.Bytes32][]byte
	leafCache    *TrieLeafCache
	bn           uint32
	journal      []*TrieJournalItem
}

func newTrie(
	store kv.Store,
	name string,
	root thor.Bytes32,
	cache *trieCache,
	secure bool,
	bn uint32,
	leafCache *TrieLeafCache,
) *Trie {
	var (
		tr = &Trie{
			store:        store,
			name:         name,
			originalRoot: root,
			cache:        cache,
			keyBuf:       newTrieNodeKeyBuf(name),
			secure:       secure,
			leafCache:    leafCache,
			bn:           bn,
		}
		trieObj *trie.Trie // the real trie object
		initErr error
	)

	tr.lazyInit = func() (*trie.Trie, error) {
		if trieObj == nil && initErr == nil {
			trieObj, initErr = trie.NewVersioned(root, &struct {
				trie.Database
				getExFunc
				directGetFunc
			}{
				nil, // leave out trie.Database, since here provides trie.DatabaseReaderEx impl
				tr.getEx,
				tr.directGet,
			}, bn>>4)
		}
		return trieObj, initErr
	}
	return tr
}

func (t *Trie) directGet(key []byte, nodeVer uint32) ([]byte, []byte, bool, error) {
	if t.leafCache != nil {
		key = append([]byte(t.name), key...)
		return t.leafCache.Get(t.bn>>4, nodeVer, key)
	}
	return nil, nil, false, nil
}

func (t *Trie) Name() string {
	return t.name
}

func (t *Trie) Journal() []*TrieJournalItem {
	return t.journal
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *Trie) Get(key []byte) ([]byte, error) {
	obj, err := t.lazyInit()
	if err != nil {
		return nil, err
	}
	return obj.TryGet(t.hashKey(key, false))
}

func (t *Trie) GetExtra(key []byte) ([]byte, []byte, error) {
	obj, err := t.lazyInit()
	if err != nil {
		return nil, nil, err
	}
	return obj.TryGetExtra(t.hashKey(key, false))
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *Trie) Update(key, val []byte) error {
	obj, err := t.lazyInit()
	if err != nil {
		return err
	}
	k := t.hashKey(key, true)
	t.journal = append(t.journal, &TrieJournalItem{
		append([]byte(nil), k...),
		append([]byte(nil), val...),
		nil,
	})
	return obj.TryUpdate(k, val)
}

func (t *Trie) UpdateExtra(key, val, extra []byte) error {
	obj, err := t.lazyInit()
	if err != nil {
		return err
	}
	k := t.hashKey(key, true)
	t.journal = append(t.journal, &TrieJournalItem{
		append([]byte(nil), k...),
		append([]byte(nil), val...),
		append([]byte(nil), extra...),
	})
	return obj.TryUpdateExtra(k, val, extra)
}

// Hash returns the root hash of the trie.
func (t *Trie) Hash() thor.Bytes32 {
	obj, err := t.lazyInit()
	if err != nil {
		// here return original root is ok, since we can
		// confirm that there's no preceding successful update.
		return t.originalRoot
	}
	return obj.Hash()
}

// Commit writes all nodes to the trie's database.
func (t *Trie) Commit() (root thor.Bytes32, err error) {
	return t.commit(t.bn + 1)
}

func (t *Trie) CommitVer(newBN uint32) (root thor.Bytes32, err error) {
	return t.commit(newBN)
}

func (t *Trie) commit(newBN uint32) (root thor.Bytes32, err error) {
	obj, err := t.lazyInit()
	if err != nil {
		return
	}

	err = t.store.Batch(func(putter kv.PutFlusher) error {
		root, err = t.doCommit(putter, obj, newBN)
		return err
	})
	if err == nil {
		t.bn = newBN
		t.journal = nil
	}
	return
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key
func (t *Trie) NodeIterator(start []byte) trie.NodeIterator {
	obj, err := t.lazyInit()
	if err != nil {
		return &errorIterator{err}
	}
	return obj.NodeIterator(start)
}

// GetKeyPreimage returns the blake2b preimage of a hashed key that was
// previously used to store a value.
func (t *Trie) GetKeyPreimage(hash thor.Bytes32) []byte {
	if key, ok := t.secureKeys[hash]; ok {
		return key
	}

	dbKey := [1 + 32]byte{trieSecureKeySpace}
	copy(dbKey[1:], hash[:])
	key, _ := t.store.Get(dbKey[:])
	return key
}

func (t *Trie) hashKey(key []byte, save bool) []byte {
	// short circute for non-secure trie.
	if !t.secure {
		return key
	}

	h := thor.Blake2b(key)
	if save {
		if t.secureKeys == nil {
			t.secureKeys = make(map[thor.Bytes32][]byte)
		}
		// have to make a copy because the key can be modified later.
		t.secureKeys[h] = append([]byte(nil), key...)
	}
	return h[:]
}

func (t *Trie) getEx(key *trie.CompositKey) (val trie.CompositValue, err error) {
	t.keyBuf.SetParts(key.Ver, key.Path, key.Hash)

	// retrieve from cache
	val = t.cache.Get(t.keyBuf[1:], len(key.Path), key.Scaning)
	if len(val) == 0 {
		// It's important to use snapshot here.
		// Getting an encoded node from db may have at most 2 get ops. Snapshot
		// can prevent parallel node deletions by trie pruner.
		if err = t.store.Snapshot(func(getter kv.Getter) error {
			t.keyBuf.SetHot()
			val, err = getter.Get(t.keyBuf[:])
			if err == nil {
				return nil
			}

			t.keyBuf.SetCold()
			val, err = getter.Get(t.keyBuf[:])
			return err
		}); err != nil {
			return
		}
		// skip caching when scaning(iterating) a trie, to prevent the cache from
		// being over filled.
		t.cache.Set(t.keyBuf[1:], val, len(key.Path))
	}
	return
}

func (t *Trie) doCommit(putter kv.Putter, trieObj *trie.Trie, newBN uint32) (root thor.Bytes32, err error) {
	// save secure key preimages
	if len(t.secureKeys) > 0 {
		buf := [1 + 32]byte{trieSecureKeySpace}
		for h, p := range t.secureKeys {
			copy(buf[1:], h[:])
			if err = putter.Put(buf[:], p); err != nil {
				return
			}
		}
		t.secureKeys = nil
	}

	return trieObj.CommitVersionedTo(&struct {
		trie.DatabaseWriter
		putExFunc
	}{
		nil, // leave out trie.DatabaseWriter, because here provides trie.DatabaseWriterEx
		// implements trie.DatabaseWriterEx.PutEncoded
		func(key *trie.CompositKey, val trie.CompositValue) error {
			t.keyBuf.SetParts(key.Ver, key.Path, key.Hash)

			t.cache.Set(t.keyBuf[1:], val, len(key.Path))
			t.keyBuf.SetHot()
			return putter.Put(t.keyBuf[:], val)
		},
	}, newBN>>4)
}

// errorIterator an iterator always in error state.
type errorIterator struct {
	err error
}

func (i *errorIterator) Next(bool) bool                { return false }
func (i *errorIterator) Error() error                  { return i.err }
func (i *errorIterator) Hash() thor.Bytes32            { return thor.Bytes32{} }
func (i *errorIterator) Node(func([]byte) error) error { return i.err }
func (i *errorIterator) Extra() []byte                 { return nil }
func (i *errorIterator) Ver() uint32                   { return 0 }
func (i *errorIterator) Parent() thor.Bytes32          { return thor.Bytes32{} }
func (i *errorIterator) Path() []byte                  { return nil }
func (i *errorIterator) Leaf() bool                    { return false }
func (i *errorIterator) LeafKey() []byte               { return nil }
func (i *errorIterator) LeafBlob() []byte              { return nil }
func (i *errorIterator) LeafProof() [][]byte           { return nil }
