package muxdb

import (
	"context"

	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

type PruningStats struct {
}

type HandleTrieLeafFunc func(key, blob1, blob2 []byte) error

type TriePruner struct {
	ctx        context.Context
	db         *MuxDB
	staleSpace byte
}

func (p *TriePruner) Perm(name string, root1, root2 thor.Bytes32, handleLeaf HandleTrieLeafFunc) error {
	t1, err := p.db.NewTrie(name, root1, false)
	if err != nil {
		return err
	}

	t2, err := p.db.NewTrie(name, root2, false)
	if err != nil {
		return err
	}

	it, _ := trie.NewDifferenceIterator(t1.NodeIterator(nil), t2.NodeIterator(nil))
	for it.Next(true) {
		if h := it.Hash(); !h.IsZero() {
			enc, err := it.Node()
			if err != nil {
				return err
			}
			_ = enc
		}
		if handleLeaf != nil {
			if it.Leaf() {
				blob1, err := t1.Get(it.LeafKey())
				if err != nil {
					return err
				}
				if err := handleLeaf(it.LeafKey(), blob1, it.LeafBlob()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *TriePruner) Prune() {

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
