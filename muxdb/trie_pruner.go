package muxdb

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vechain/thor/muxdb/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

const (
	flushThreshold = 4096
)

type PruningStats struct {
}

type HandleTrieLeafFunc func(key, blob1, blob2 []byte) error

type TriePruner struct {
	ctx          context.Context
	db           *MuxDB
	staleSpace   byte
	queuedWrites []*struct{ k, v []byte }
}

func (p *TriePruner) Perm(name string, root1, root2 thor.Bytes32, handleLeaf HandleTrieLeafFunc) (int, int, error) {
	nodeCount := 0
	entryCount := 0
	nodeBkt := newTrieNodeBucket(name)

	t1, err := p.db.NewTrie(name, root1, false)
	if err != nil {
		return 0, 0, err
	}

	t2, err := p.db.NewTrie(name, root2, false)
	if err != nil {
		return 0, 0, err
	}

	it, _ := trie.NewDifferenceIterator(t1.NodeIterator(nil), t2.NodeIterator(nil))
	for it.Next(true) {
		if h := it.Hash(); !h.IsZero() {
			enc, err := it.Node()
			if err != nil {
				return 0, 0, err
			}
			nk := &trie.NodeKey{
				Hash: h[:],
				Path: it.Path(),
			}
			if err := nodeBkt.Put(p.enqueuePut, nk, enc, triePermanentSpace); err != nil {
				return 0, 0, err
			}
			nodeCount++
		}
		if it.Leaf() {
			entryCount++
			if handleLeaf != nil {
				blob1, err := t1.Get(it.LeafKey())
				if err != nil {
					return 0, 0, err
				}
				blob2 := it.LeafBlob()
				if err := handleLeaf(it.LeafKey(), blob1, blob2); err != nil {
					return 0, 0, err
				}
			}
		}
	}
	return nodeCount, entryCount, it.Error()
}

func (p *TriePruner) Prune() (int, error) {
	delCount := 0
	rng := kv.Range(*util.BytesPrefix([]byte{p.staleSpace}))
	for {
		n := 0
		var writeError error
		if err := p.db.engine.Iterate(rng, func(pair kv.Pair) bool {
			if n >= flushThreshold*16 {
				rng.Start = append([]byte(nil), pair.Key()...)
				return false
			}
			n++
			writeError = p.enqueueDel(pair.Key())
			if writeError != nil {
				return false
			}
			delCount++
			return true
		}); err != nil {
			return 0, err
		}
		if writeError != nil {
			return 0, writeError
		}
		if n == 0 {
			break
		}

	}
	return delCount, p.flushQueue()
}

func (p *TriePruner) enqueuePut(k, v []byte) error {
	p.queuedWrites = append(p.queuedWrites, &struct{ k, v []byte }{
		append([]byte(nil), k...),
		append([]byte(nil), v...),
	})
	if len(p.queuedWrites) >= flushThreshold {
		return p.flushQueue()
	}
	return nil
}

func (p *TriePruner) enqueueDel(k []byte) error {
	p.queuedWrites = append(p.queuedWrites, &struct{ k, v []byte }{
		k: append([]byte(nil), k...),
	})
	if len(p.queuedWrites) >= flushThreshold {
		return p.flushQueue()
	}
	return nil
}

func (p *TriePruner) flushQueue() error {
	if len(p.queuedWrites) > 0 {
		if err := p.db.engine.Batch(func(putter kv.Putter) error {
			for _, kv := range p.queuedWrites {
				if len(kv.v) > 0 {
					if err := putter.Put(kv.k, kv.v); err != nil {
						return err
					}
				} else {
					if err := putter.Delete(kv.k); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		p.queuedWrites = p.queuedWrites[:0]
	}
	return nil
}

type triePruningSeq int32

func (s triePruningSeq) CurrentCommitSpace() byte {
	if s%2 == 0 {
		return trieCommitSpaceA
	}
	return trieCommitSpaceB
}

func (s triePruningSeq) StaleCommitSpace() byte {
	if s%2 == 0 {
		return trieCommitSpaceB
	}
	return trieCommitSpaceA
}
