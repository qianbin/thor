// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/trie"
)

const (
	prunerBatchSize = 4096
)

// HandleTrieLeafFunc callback function to handle trie leaf.
type HandleTrieLeafFunc func(key, blob1, extra1, blob2, extra2 []byte) error

// TriePruner is the trie pruner.
type TriePruner struct {
	db *MuxDB
}

func newTriePruner(db *MuxDB) *TriePruner {
	return &TriePruner{
		db,
	}
}

// ArchiveNodes save differential nodes of two tries into permanent space.
// handleLeaf can be nil if not interested.
func (p *TriePruner) ArchiveNodes(
	ctx context.Context,
	trie1, trie2 *Trie,
	handleLeaf HandleTrieLeafFunc,
) (nodeCount int, entryCount int, err error) {
	ita, itb := trie1.NodeIterator(nil), trie2.NodeIterator(nil)
	it, _ := trie.NewDifferenceIterator(ita, itb)

	err = p.db.engine.Batch(func(putter kv.PutFlusher) error {
		keyBuf := newTrieNodeKeyBuf(trie1.Name())
		for it.Next(true) {
			if h := it.Hash(); !h.IsZero() {
				keyBuf.SetParts(it.Ver(), it.Path(), h[:])
				keyBuf.SetCold()
				err := it.Node(func(enc []byte) error {
					return putter.Put(keyBuf[:], enc)
				})
				if err != nil {
					return err
				}

				nodeCount++
				if nodeCount > 0 && nodeCount%prunerBatchSize == 0 {
					if err := putter.Flush(); err != nil {
						return err
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
				}
			}

			if it.Leaf() {
				entryCount++
				if handleLeaf != nil {
					var blob1, extra1 []byte
					if ita.Leaf() && bytes.Equal(ita.Path(), itb.Path()) {
						blob1 = ita.LeafBlob()
						extra1 = ita.Extra()
					}
					// blob1, extra1, err := trie1.GetExtra(it.LeafKey())
					// if err != nil {
					// 	return err
					// }
					blob2 := it.LeafBlob()
					extra2 := it.Extra()
					if err := handleLeaf(it.LeafKey(), blob1, extra1, blob2, extra2); err != nil {
						return err
					}
				}
			}
		}
		return it.Error()
	})

	return
}

// DropStaleNodes delete stale trie nodes.
func (p *TriePruner) DropStaleNodes(ctx context.Context, limitBlockNum uint32) (count int, err error) {
	err = p.db.engine.Batch(func(putter kv.PutFlusher) error {
		var limit [5]byte
		limit[0] = trieHotSpace

		binary.BigEndian.PutUint32(limit[1:], limitBlockNum>>8)

		rng := kv.Range{
			Start: []byte{trieHotSpace},
			Limit: limit[:],
		}
		var nextStart []byte
		for {
			iterCount := 0
			// use short-range iterator here to prevent from holding snapshot for long time.
			if err := p.db.engine.Iterate(rng, func(pair kv.Pair) bool {
				iterCount++
				nextStart = append(append(nextStart[:0], pair.Key()...), 0)

				// error can be ignored here
				_ = putter.Delete(pair.Key())

				return iterCount < prunerBatchSize
			}); err != nil {
				return err
			}

			// no more
			if iterCount == 0 {
				break
			}
			count += iterCount
			if err := putter.Flush(); err != nil {
				return err
			}
			rng.Start = nextStart

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
	return
}
