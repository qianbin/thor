package muxdb

import (
	"github.com/vechain/thor/muxdb/kv"
	"github.com/vechain/thor/trie"
)

type bucket []byte

func (b bucket) ProxyGetter(getter kv.Getter) kv.Getter {
	return &struct {
		kv.GetFunc
		kv.HasFunc
	}{
		func(key []byte) ([]byte, error) {
			return getter.Get(b.makeKey(key))
		},
		func(key []byte) (bool, error) {
			return getter.Has(b.makeKey(key))
		},
	}
}

func (b bucket) ProxyPutter(putter kv.Putter) kv.Putter {
	return &struct {
		kv.PutFunc
		kv.DeleteFunc
	}{
		func(key, val []byte) error {
			return putter.Put(b.makeKey(key), val)
		},
		func(key []byte) error {
			return putter.Delete(b.makeKey(key))
		},
	}
}

func (b bucket) TransformRange(r kv.Range) kv.Range {
	return kv.Range{
		Start: b.makeKey(r.Start),
		Limit: b.makeKey(r.Limit),
	}
}
func (b bucket) TransformPair(pair kv.Pair) kv.Pair {
	return &struct {
		kv.KeyFunc
		kv.ValueFunc
	}{
		func() []byte {
			// mask key's bucket prefix
			return pair.Key()[len(b):]
		},
		pair.Value,
	}
}

func (b bucket) makeKey(key []byte) []byte {
	newKey := make([]byte, 0, len(b)+len(key))
	return append(append(newKey, b...), key...)
}

type trieNodeBucket struct {
	fullKey []byte

	space       *byte
	compactPath []byte
	hash        []byte
}

func newTrieNodeBucket(name string) *trieNodeBucket {
	nameLen := len(name)
	// the trie node key is composed in this way:
	// [trieSpace, name, encoded-path, node-hash]
	keyBuf := make([]byte, 1+nameLen+8+32)
	copy(keyBuf[1:], name)

	return &trieNodeBucket{
		keyBuf,
		&keyBuf[0],
		keyBuf[1+nameLen:][:8],
		keyBuf[1+nameLen+8:],
	}
}

func (b *trieNodeBucket) Get(get kv.GetFunc, key *trie.NodeKey) ([]byte, error) {
	b.setPath(key.Path)
	copy(b.hash, key.Hash)

	*b.space = triePermanentSpace
	if val, err := get(b.fullKey); err == nil {
		return val, nil
	}

	*b.space = trieCommitSpaceA
	if val, err := get(b.fullKey); err == nil {
		return val, nil
	}

	*b.space = trieCommitSpaceB
	return get(b.fullKey)
}

func (b *trieNodeBucket) Put(put kv.PutFunc, key *trie.NodeKey, val []byte, space byte) error {
	b.setPath(key.Path)
	copy(b.hash, key.Hash)
	*b.space = space

	return put(b.fullKey, val)
}

func (b *trieNodeBucket) setPath(path []byte) {
	for i := 0; i < 8; i++ {
		b.compactPath[i] = 0
	}

	pathLen := len(path)
	if pathLen > 15 {
		pathLen = 15
	}

	// compact at most 15 nibbles and term with path len
	for i := 0; i < pathLen; i++ {
		if i%2 == 0 {
			b.compactPath[i/2] |= (path[i] << 4)
		} else {
			b.compactPath[i/2] |= path[i]
		}
	}
	b.compactPath[7] |= byte(pathLen)
}
