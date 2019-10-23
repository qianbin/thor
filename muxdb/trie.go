package muxdb

import (
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

type Trie interface {
	Get(key []byte) ([]byte, error)
	Update(key, val []byte) error
	Hash() (thor.Bytes32, error)
	Commit() (thor.Bytes32, error)
	NodeIterator(start []byte) (trie.NodeIterator, error)

	Prefix() []byte
}

type trieWrap struct {
	raw     *trie.Trie
	err     error
	hashKey func(key []byte, save bool) []byte
	batch   func(func(putFunc) error) error

	name string
	seg  segment
}

func (t *trieWrap) Get(key []byte) ([]byte, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.raw.TryGet(t.hashKey(key, false))
}

func (t *trieWrap) Update(key, val []byte) error {
	if t.err != nil {
		return t.err
	}
	return t.raw.TryUpdate(t.hashKey(key, true), val)
}

func (t *trieWrap) Hash() (thor.Bytes32, error) {
	if t.err != nil {
		return thor.Bytes32{}, t.err
	}
	return t.raw.Hash(), nil
}

func (t *trieWrap) Commit() (thor.Bytes32, error) {
	if t.err != nil {
		return thor.Bytes32{}, t.err
	}

	var (
		root thor.Bytes32
		err  error
	)

	err = t.batch(func(put putFunc) error {
		root, err = t.raw.CommitTo(struct{ putFunc }{put})
		return err
	})

	if err != nil {
		return thor.Bytes32{}, err
	}
	return root, nil
}

func (t *trieWrap) NodeIterator(start []byte) (trie.NodeIterator, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.raw.NodeIterator(start), nil
}

func (t *trieWrap) Prefix() []byte {
	return append(append([]byte{trieSlot}, []byte(t.name)...), byte(t.seg>>8), byte(t.seg))
}
