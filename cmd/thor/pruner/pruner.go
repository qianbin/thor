// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package pruner

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/inconshreveable/log15"
	"github.com/vechain/thor/block"
	"github.com/vechain/thor/chain"
	"github.com/vechain/thor/co"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/state"
	"github.com/vechain/thor/thor"
)

var log = log15.New("pkg", "pruner")

const (
	propsStoreName = "pruner.props"
	statusKey      = "status"

	stepInitiate           = ""
	stepArchiveIndexTrie   = "archiveIndexTrie"
	stepArchiveAccountTrie = "archiveAccountTrie"
	stepDropStale          = "dropStale"
)

// Pruner is the state pruner.
type Pruner struct {
	db     *muxdb.MuxDB
	repo   *chain.Repository
	ctx    context.Context
	cancel func()
	goes   co.Goes
}

// New creates and starts a state pruner.
func New(db *muxdb.MuxDB, repo *chain.Repository) *Pruner {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pruner{
		db:     db,
		repo:   repo,
		ctx:    ctx,
		cancel: cancel,
	}
	p.goes.Go(func() {
		if err := p.loop(); err != nil {
			if err != context.Canceled {
				log.Warn("pruner interrupted", "error", err)
			}
		}
	})
	p.goes.Go(func() {
		if err := p.xx(); err != nil {
			if err != context.Canceled {
				log.Warn("xx interrupted", "error", err)
			}
		}
	})
	return p
}

// Stop stops the state pruner.
func (p *Pruner) Stop() {
	p.cancel()
	p.goes.Wait()
}

func (p *Pruner) xx() error {
	lc := p.db.LeafCache()
	const step = 1 << 8

	go func() {
		for {
			<-time.After(20 * time.Second)
			log.Info("LeafCache", "range", lc.Ver()*16)
		}
	}()

	v := lc.Ver()

	for {
		n := (v) << 8
		nn := (v + 1) << 8
		bc := p.repo.NewBestChain()
		if n+step+128 < block.Number(bc.HeadID()) {
			j := p.db.NewBucket([]byte{muxdb.TrieJournalSpace})
			var tmp1 [4]byte
			binary.BigEndian.PutUint32(tmp1[:], n)
			var tmp2 [4]byte
			binary.BigEndian.PutUint32(tmp2[:], nn)
			rng := kv.Range{
				Start: tmp1[:],
				Limit: tmp2[:],
			}

			var err error

			if err = j.Iterate(rng, func(pair kv.Pair) bool {
				k := pair.Key()
				var ok bool

				ok, err = bc.HasBlock(thor.BytesToBytes32(k[:32]))
				if err != nil {
					return false
				}

				if ok {
					lc.Put(k[32:], pair.Value())
				} else {
					fmt.Println("skipped")
				}
				return true
			}); err != nil {
				return err
			}
			if err := lc.Flush(v); err != nil {
				return err
			}
			v++
			if err = j.Iterate(rng, func(pair kv.Pair) bool {
				i := 0
				err = j.Batch(func(p kv.PutFlusher) error {
					if err := p.Delete(pair.Key()); err != nil {
						return err
					}
					i++
					if i%500 == 0 {
						if err := p.Flush(); err != nil {
							return err
						}
					}
					return nil
				})
				return true
			}); err != nil {
				return err
			}
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
			}
		} else {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}
func (p *Pruner) loop() error {
	var status status
	if err := status.Load(p.db); err != nil {
		return err
	}
	if status.Cycles == 0 && status.Step == stepInitiate {
		log.Info("pruner started")
	} else {
		log.Info("pruner started", "range", fmt.Sprintf("[%v, %v]", status.N1, status.N2), "step", status.Step)
	}

	pruner := p.db.NewTriePruner()

	if status.Cycles == 0 {
		if _, _, err := p.archiveIndexTrie(pruner, 0, 0); err != nil {
			return err
		}
		if _, _, _, _, err := p.archiveAccountTrie(pruner, 0, 0); err != nil {
			return err
		}
	}

	bestNum := func() uint32 {
		return p.repo.BestBlock().Header().Number()
	}

	waitUntil := func(n uint32) error {
		for {
			if bestNum() > n {
				return nil
			}
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}

	for {
		switch status.Step {
		case stepInitiate:
			// if err := pruner.SwitchLiveSpace(); err != nil {
			// 	return err
			// }
			status.N1 = status.N2
			status.N2 += 8192
			// not necessary to prune if n2 is too small
			// if status.N2 < thor.MaxStateHistory {
			// 	status.N2 = thor.MaxStateHistory
			// }
			if err := waitUntil(status.N2 + 128); err != nil {
				return err
			}
			log.Info("initiated", "range", fmt.Sprintf("[%v, %v]", status.N1, status.N2))
			status.Step = stepArchiveIndexTrie
		case stepArchiveIndexTrie:
			log.Info("archiving index trie...")
			nodeCount, entryCount, err := p.archiveIndexTrie(pruner, status.N1, status.N2)
			if err != nil {
				return err
			}
			log.Info("archived index trie", "nodes", nodeCount, "entries", entryCount)
			status.Step = stepArchiveAccountTrie
		case stepArchiveAccountTrie:
			log.Info("archiving account trie...")
			nodeCount, entryCount, sNodeCount, sEntryCount, err := p.archiveAccountTrie(pruner, status.N1, status.N2)
			if err != nil {
				return err
			}
			log.Info("archived account trie",
				"nodes", nodeCount, "entries", entryCount,
				"storageNodes", sNodeCount, "storageEntries", sEntryCount)
			status.Step = stepDropStale
		case stepDropStale:
			if err := waitUntil(status.N2 + thor.MaxStateHistory + 128); err != nil {
				return err
			}
			log.Info("sweeping stale nodes...")
			count, err := pruner.DropStaleNodes(p.ctx, status.N2)
			if err != nil {
				return err
			}
			log.Info("swept stale nodes", "count", count)

			status.Cycles++
			status.Step = stepInitiate
		default:
			return fmt.Errorf("unexpected pruner step: %v", status.Step)
		}

		if err := status.Save(p.db); err != nil {
			return err
		}
	}
}

// func (p *Pruner) writeLeafCache(leafCache *muxdb.TrieLeafCache, trie1, trie2 *muxdb.Trie, handleLeaf func(key, blob1, extra1, blob2, extra2 []byte) error) error {
// 	nameLen := len(trie1.Name())
// 	keyBuf := []byte(trie1.Name())

// 	it, _ := trie.NewDifferenceIterator(trie1.NodeIterator(nil), trie2.NodeIterator(nil))
// 	itCount1 := 0
// 	for it.Next(true) {
// 		itCount1++
// 		if it.Leaf() {
// 			keyBuf = append(keyBuf[:nameLen], it.LeafKey()...)
// 			leafCache.Put(keyBuf, it.LeafBlob(), it.Extra())
// 			if handleLeaf != nil {
// 				blob1, extra1, err := trie1.GetExtra(it.LeafKey())
// 				if err != nil {
// 					return err
// 				}
// 				blob2 := it.LeafBlob()
// 				extra2 := it.Extra()
// 				if err := handleLeaf(it.LeafKey(), blob1, extra1, blob2, extra2); err != nil {
// 					return err
// 				}
// 			}
// 		}
// 	}
// 	if err := it.Error(); err != nil {
// 		return err
// 	}
// 	itCount2 := 0
// 	it, _ = trie.NewDifferenceIterator(trie2.NodeIterator(nil), trie1.NodeIterator(nil))
// 	for it.Next(true) {
// 		itCount2++
// 		if it.Leaf() {
// 			keyBuf = append(keyBuf[:nameLen], it.LeafKey()...)
// 			leafCache.MarkNilIfAbsent(keyBuf)
// 		}
// 	}

// 	return it.Error()
// }

func (p *Pruner) archiveIndexTrie(pruner *muxdb.TriePruner, n1, n2 uint32) (nodeCount, entryCount int, err error) {
	var (
		bestChain              = p.repo.NewBestChain()
		id1, id2, root1, root2 thor.Bytes32
		s1, s2                 *chain.BlockSummary
	)

	if id1, err = bestChain.GetBlockID(n1); err != nil {
		return
	}
	if s1, err = p.repo.GetBlockSummary(id1); err != nil {
		return
	}
	root1 = s1.IndexRoot

	if id2, err = bestChain.GetBlockID(n2); err != nil {
		return
	}
	if s2, err = p.repo.GetBlockSummary(id2); err != nil {
		return
	}
	root2 = s2.IndexRoot

	if n1 == 0 && n2 == 0 {
		root1 = thor.Bytes32{}
	}
	t1 := p.db.NewTrie(chain.IndexTrieName, root1, n1)
	t2 := p.db.NewTrie(chain.IndexTrieName, root2, n2)
	nodeCount, entryCount, err = pruner.ArchiveNodes(p.ctx, t1, t2, nil)
	return
}

func (p *Pruner) archiveAccountTrie(pruner *muxdb.TriePruner, n1, n2 uint32) (nodeCount, entryCount, storageNodeCount, storageEntryCount int, err error) {
	var (
		bestChain        = p.repo.NewBestChain()
		header1, header2 *block.Header
		root1, root2     thor.Bytes32
	)

	if header1, err = bestChain.GetBlockHeader(n1); err != nil {
		return
	}
	if header2, err = bestChain.GetBlockHeader(n2); err != nil {
		return
	}
	root1, root2 = header1.StateRoot(), header2.StateRoot()
	if n1 == 0 && n2 == 0 {
		root1 = thor.Bytes32{}
	}

	t1 := p.db.NewTrie(state.AccountTrieName, root1, n1)
	t2 := p.db.NewTrie(state.AccountTrieName, root2, n2)

	nodeCount, entryCount, err = pruner.ArchiveNodes(p.ctx, t1, t2, func(key, blob1, extra1, blob2, extra2 []byte) error {
		var sRoot1, sRoot2 thor.Bytes32
		var sv1, sv2 uint32
		if len(blob1) > 0 {
			var acc state.Account
			if err := rlp.DecodeBytes(blob1, &acc); err != nil {
				return err
			}
			sRoot1 = thor.BytesToBytes32(acc.StorageRoot)
			if len(extra1) > 0 {
				rlp.DecodeBytes(extra1, &sv1)
			}
		}
		if len(blob2) > 0 {
			var acc state.Account
			if err := rlp.DecodeBytes(blob2, &acc); err != nil {
				return err
			}
			sRoot2 = thor.BytesToBytes32(acc.StorageRoot)
			if len(extra2) > 0 {
				rlp.DecodeBytes(extra2, &sv2)
			}
		}
		if !sRoot2.IsZero() {
			tn := state.StorageTrieName(thor.BytesToBytes32(key))
			// fmt.Println(sRoot1, sv1)
			// fmt.Println(sRoot2, sv2)
			t1 := p.db.NewTrie(tn, sRoot1, sv1)
			t2 := p.db.NewTrie(tn, sRoot2, sv2)
			n, e, err := pruner.ArchiveNodes(p.ctx, t1, t2, nil)
			if err != nil {
				return err
			}
			storageNodeCount += n
			storageEntryCount += e
		}
		// }
		return nil
	})
	return
}
