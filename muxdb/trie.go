package muxdb

import (
	"github.com/vechain/thor/muxdb/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

// Single functions of trie advanced db interface.
type (
	getEncodedFunc func(key *trie.NodeKey) ([]byte, error)
	getDecodedFunc func(key *trie.NodeKey) (interface{}, func(interface{}))
	putEncodedFunc func(key *trie.NodeKey, enc []byte) error
)

func (f getEncodedFunc) GetEncoded(key *trie.NodeKey) ([]byte, error)                  { return f(key) }
func (f getDecodedFunc) GetDecoded(key *trie.NodeKey) (interface{}, func(interface{})) { return f(key) }
func (f putEncodedFunc) PutEncoded(key *trie.NodeKey, enc []byte) error                { return f(key, enc) }

var (
	_ trie.DatabaseReaderEx = (*struct {
		getEncodedFunc
		getDecodedFunc
	})(nil)
	_ trie.DatabaseWriterEx = putEncodedFunc(nil)
)

// Trie defines trie interface.
type Trie interface {
	Get(key []byte) ([]byte, error)
	Update(key, val []byte) error
	Hash() thor.Bytes32
	Commit() (thor.Bytes32, error)
	NodeIterator(start []byte) trie.NodeIterator
	GetKeyPreimage(hash thor.Bytes32) []byte
}

type managedTrie struct {
	obj         *trie.Trie
	engine      kv.Engine
	cache       *trieCache
	nodeBucket  *trieNodeBucket
	secureKeys  map[thor.Bytes32][]byte
	commitSpace func() byte
}

func newManagedTrie(
	engine kv.Engine,
	cache *trieCache,
	name string,
	root thor.Bytes32,
	secure bool,
	commitSpace func() byte,
) (*managedTrie, error) {
	nodeBucket := newTrieNodeBucket(name)
	obj, err := trie.New(root, &struct {
		kv.GetFunc
		kv.HasFunc
		kv.PutFunc
		getEncodedFunc
		getDecodedFunc
	}{
		nil, nil, nil,
		// GetEncoded
		func(key *trie.NodeKey) ([]byte, error) {
			enc := cache.GetEncoded(key.Hash, len(key.Path), key.Scaning)
			if len(enc) > 0 {
				return enc, nil
			}

			var err error
			err = engine.Snapshot(func(getter kv.Getter) error {
				enc, err = nodeBucket.Get(getter.Get, key)
				return err
			})
			if err != nil {
				return nil, err
			}

			if !key.Scaning {
				cache.SetEncoded(key.Hash, enc, len(key.Path))
			}
			return enc, nil
		},
		// GetDecoded
		func(key *trie.NodeKey) (interface{}, func(interface{})) {
			if len(key.Path) > 3 {
				return nil, nil
			}
			if cached := cache.GetDecoded(key.Hash, key.Scaning); cached != nil {
				return cached, nil
			}
			if !key.Scaning {
				// fill cache only if not iterating
				return nil, func(dec interface{}) { cache.SetDecoded(key.Hash, dec) }
			}
			return nil, nil
		},
	})
	if err != nil {
		return nil, err
	}

	trie := &managedTrie{
		obj:         obj,
		engine:      engine,
		cache:       cache,
		nodeBucket:  nodeBucket,
		commitSpace: commitSpace,
	}
	if secure {
		trie.secureKeys = make(map[thor.Bytes32][]byte)
	}
	return trie, nil
}

func (t *managedTrie) Get(key []byte) ([]byte, error) {
	return t.obj.TryGet(t.hashKey(key, false))
}

func (t *managedTrie) Update(key, val []byte) error {
	return t.obj.TryUpdate(t.hashKey(key, true), val)
}

func (t *managedTrie) Hash() thor.Bytes32 {
	return t.obj.Hash()
}

func (t *managedTrie) Commit() (thor.Bytes32, error) {
	var (
		root        thor.Bytes32
		err         error
		commitSpace = t.commitSpace()
	)

	err = t.engine.Batch(func(putter kv.Putter) error {
		if t.secureKeys != nil {
			dbKey := [1 + 32]byte{trieSecureKeySpace}
			for h, p := range t.secureKeys {
				copy(dbKey[1:], h[:])
				if err := putter.Put(dbKey[:], p); err != nil {
					return err
				}
			}
			t.secureKeys = nil
		}
		root, err = t.obj.CommitTo(struct {
			kv.PutFunc
			putEncodedFunc
		}{
			nil,
			func(key *trie.NodeKey, enc []byte) error {
				t.cache.SetEncoded(key.Hash, enc, len(key.Path))
				return t.nodeBucket.Put(putter.Put, key, enc, commitSpace)
			},
		})
		return err
	})

	return root, err
}

func (t *managedTrie) NodeIterator(start []byte) trie.NodeIterator {
	return t.obj.NodeIterator(start)
}

func (t *managedTrie) GetKeyPreimage(hash thor.Bytes32) []byte {
	if t.secureKeys != nil {
		if key, ok := t.secureKeys[hash]; ok {
			return key
		}
	}
	dbKey := [1 + 32]byte{trieSecureKeySpace}
	copy(dbKey[1:], hash[:])
	key, _ := t.engine.Get(dbKey[:])
	return key
}

func (t *managedTrie) hashKey(key []byte, updating bool) []byte {
	if t.secureKeys == nil {
		return key
	}

	h := thor.Blake2b(key)
	if updating {
		t.secureKeys[h] = append([]byte(nil), key...)
	}
	return h[:]
}
