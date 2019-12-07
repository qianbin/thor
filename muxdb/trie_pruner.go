package muxdb

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vechain/thor/muxdb/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

const (
	pruneBatchSize = 4096
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

func (p *TriePruner) SaveDiff(name string, root1, root2 thor.Bytes32, handleLeaf HandleTrieLeafFunc) (nodeCount int, entryCount int, err error) {
	var trie1, trie2 Trie
	if trie1, err = p.db.NewTrie(name, root1, false); err != nil {
		return
	}
	if trie2, err = p.db.NewTrie(name, root2, false); err != nil {
		return
	}

	it, _ := trie.NewDifferenceIterator(trie1.NodeIterator(nil), trie2.NodeIterator(nil))

	err = p.db.engine.Batch(func(putter kv.PutCommitter) error {
		nodeBkt := newTrieNodeBucket(name)
		for it.Next(true) {
			if h := it.Hash(); !h.IsZero() {
				enc, err := it.Node()
				if err != nil {
					return err
				}
				nk := &trie.NodeKey{
					Hash: h[:],
					Path: it.Path(),
				}
				if err := nodeBkt.Put(putter.Put, nk, enc, triePermanentSpace); err != nil {
					return err
				}
				nodeCount++
				if nodeCount > 0 && nodeCount%pruneBatchSize == 0 {
					if err := putter.Commit(); err != nil {
						return err
					}
				}
			}

			if it.Leaf() {
				entryCount++
				if handleLeaf != nil {
					blob1, err := trie1.Get(it.LeafKey())
					if err != nil {
						return err
					}
					blob2 := it.LeafBlob()
					if err := handleLeaf(it.LeafKey(), blob1, blob2); err != nil {
						return err
					}
				}
			}
		}
		return it.Error()
	})
	return
}

func (p *TriePruner) Prune() (count int, err error) {
	err = p.db.engine.Batch(func(putter kv.PutCommitter) error {
		rng := kv.Range(*util.BytesPrefix([]byte{p.staleSpace}))
		for {

			hasMore := false
			if err := p.db.engine.Iterate(rng, func(pair kv.Pair) bool {
				hasMore = true
				rng.Start = append(append(rng.Start[:0], pair.Key()...), 0)
				putter.Delete(pair.Key())

				count++

				if count > 0 && count%pruneBatchSize == 0 {
					return false
				}
				return true
			}); err != nil {
				return err
			}

			if !hasMore {
				break
			}

			if err := putter.Commit(); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// func (p *TriePruner) enqueuePut(k, v []byte) error {
// 	p.queuedWrites = append(p.queuedWrites, &struct{ k, v []byte }{
// 		append([]byte(nil), k...),
// 		append([]byte(nil), v...),
// 	})
// 	if len(p.queuedWrites) >= flushThreshold {
// 		return p.flushQueue()
// 	}
// 	return nil
// }

// func (p *TriePruner) enqueueDel(k []byte) error {
// 	p.queuedWrites = append(p.queuedWrites, &struct{ k, v []byte }{
// 		k: append([]byte(nil), k...),
// 	})
// 	if len(p.queuedWrites) >= flushThreshold {
// 		return p.flushQueue()
// 	}
// 	return nil
// }

// func (p *TriePruner) flushQueue() error {
// 	if len(p.queuedWrites) > 0 {
// 		if err := p.db.engine.Batch(func(putter kv.PutCommitter) error {
// 			for _, kv := range p.queuedWrites {
// 				if len(kv.v) > 0 {
// 					if err := putter.Put(kv.k, kv.v); err != nil {
// 						return err
// 					}
// 				} else {
// 					if err := putter.Delete(kv.k); err != nil {
// 						return err
// 					}
// 				}
// 			}
// 			return nil
// 		}); err != nil {
// 			return err
// 		}
// 		p.queuedWrites = p.queuedWrites[:0]
// 	}
// 	return nil
// }

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
