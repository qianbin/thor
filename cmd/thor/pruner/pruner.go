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
	"github.com/syndtr/goleveldb/leveldb/util"
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
	// p.goes.Go(func() {
	// 	if err := p.loop(); err != nil {
	// 		if err != context.Canceled {
	// 			log.Warn("pruner interrupted", "error", err)
	// 		}
	// 	}
	// })
	p.goes.Go(func() {
		if err := p.xx(); err != nil {
			if err != context.Canceled {
				log.Warn("xx interrupted", "error", err)
			}
		}
	})
	p.goes.Go(func() {
		if err := p.pruneIndexTrie(); err != nil {
			if err != context.Canceled {
				log.Warn("pruneIndexTrie interrupted", "error", err)
			}
		}
	})
	p.goes.Go(func() {
		if err := p.pruneAccTrie(); err != nil {
			if err != context.Canceled {
				log.Warn("pruneAccountTrie interrupted", "error", err)
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
	const step = 1024

	go func() {
		for {
			<-time.After(20 * time.Second)
			log.Info("LeafCache", "range", lc.Ver())
		}
	}()

	v := lc.Ver()

	for {
		n := v
		nn := n + step
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
			v += step
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
	return nil
}

func (p *Pruner) waitUntil(n uint32) error {
	for {
		if p.repo.BestBlock().Header().Number() > n {
			return nil
		}
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (p *Pruner) saveState(name string, val interface{}) error {
	data, _ := rlp.EncodeToBytes(val)
	return p.db.NewStore("pruner-state").Put([]byte(name), data)
}

func (p *Pruner) loadState(name string, val interface{}) error {
	data, err := p.db.NewStore("pruner-state").Get([]byte(name))
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return rlp.DecodeBytes(data, val)
}

func pathToNum(path []byte) uint64 {
	n := len(path)
	if n > 15 {
		n = 15
	}

	var v uint64
	v = uint64(n)
	for i := 0; i < 15; i++ {
		v <<= 4
		if i < n {
			v |= uint64(path[i])
		}
	}
	return v
}

func (p *Pruner) cleanTrie(tr *muxdb.Trie, lastVer uint32) (int, error) {
	var (
		// path -> ver
		m           = make(map[uint64]uint32)
		deleteCount = 0
	)

	it := tr.NodeIterator(nil)

	skipChild := false
	for it.Next(!skipChild) {
		if it.Branch() {
			ver := it.Ver()
			if ver > lastVer {
				if len(it.Path()) == 0 {
					m[0] = ver
				}
				for i := 0; i < 16; i++ {
					cv := it.ChildVer(i)
					if cv > lastVer {
						m[pathToNum(append(it.Path(), byte(i)))] = cv
					}
				}
			}
			skipChild = ver <= lastVer || len(it.Path()) >= 4
		} else {
			skipChild = true
		}
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	bk := p.db.NewBucket([]byte(string(byte(0)) + tr.Name()))
	rng := kv.Range{
		// Start: []byte{0x00},
		// Limit: []byte{0x60},
	}

	if err := bk.Batch(func(putter kv.PutFlusher) error {
		var err error
		bk.Iterate(rng, func(pair kv.Pair) bool {
			k := binary.BigEndian.Uint64(pair.Key())
			if ver, ok := m[k]; ok {
				if binary.BigEndian.Uint32(pair.Key()[8:]) < ver {
					err = putter.Delete(pair.Key())
					deleteCount++
				}
			}
			if deleteCount%1000 == 0 {
				err = putter.Flush()
			}
			return err == nil
		})
		return err
	}); err != nil {
		return 0, err
	}
	return deleteCount, nil
}

func (p *Pruner) pruneIndexTrie() error {
	deleteCount := 0
	var n uint32
	p.loadState(chain.IndexTrieName, &n)
	go func() {
		for {
			<-time.After(15 * time.Second)
			log.Info("=====IndexTrie", "n", n, "delete", deleteCount)
		}
	}()

	for {
		var target uint32
		bn := p.repo.BestBlock()
		if bn.Header().Number() > 128 {
			target = bn.Header().Number() - 128
		}
		if n+1024 > target {
			target = n + 1024
		}

		if err := p.waitUntil(target + 128); err != nil {
			return err
		}
		id, err := p.repo.NewBestChain().GetBlockID(target)
		if err != nil {
			return err
		}
		sum, err := p.repo.GetBlockSummary(id)
		if err != nil {
			return err
		}
		dn, err := p.cleanTrie(p.db.NewTrie(chain.IndexTrieName, sum.IndexRoot, sum.Header.Number()), n)
		if err != nil {
			return err
		}
		deleteCount += dn
		n = target
		p.saveState(chain.IndexTrieName, n)
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}
	}
}

func (p *Pruner) pruneAccTrie() error {
	deleteCount := 0
	deleteSCount := 0
	var n uint32
	var sAddr thor.Address
	p.loadState(state.AccountTrieName, &n)
	p.loadState("s", &sAddr)
	go func() {
		for {
			<-time.After(15 * time.Second)
			log.Info("=====AccountTrie", "n", n, "delete", deleteCount, "deleteStorage", deleteSCount)
		}
	}()

	for {
		var target uint32
		bn := p.repo.BestBlock()
		if bn.Header().Number() > thor.MaxStateHistory+128 {
			target = bn.Header().Number() - thor.MaxStateHistory - 128
		}
		if n+1024 > target {
			target = n + 1024
		}

		if err := p.waitUntil(target + thor.MaxStateHistory + 128); err != nil {
			return err
		}

		header, err := p.repo.NewBestChain().GetBlockHeader(target)
		if err != nil {
			return err
		}

		dn, err := p.cleanTrie(p.db.NewTrie(state.AccountTrieName, header.StateRoot(), header.Number()), n)
		if err != nil {
			return err
		}
		deleteCount += dn
		lastN := n
		n = target
		p.saveState(state.AccountTrieName, n)
		//
		p.iterateStorage(sAddr, func(addr thor.Address) error {
			sAddr = addr
			if err := p.saveState("s", sAddr); err != nil {
				return err
			}
			accTrie := p.db.NewSecureTrie(state.AccountTrieName, header.StateRoot(), header.Number())
			data, extra, err := accTrie.GetExtra(addr[:])
			if err != nil {
				return err
			}
			if len(data) > 0 {
				var acc state.Account
				if err := rlp.DecodeBytes(data, &acc); err != nil {
					return err
				}

				var ver uint32
				if len(extra) > 0 {
					if err := rlp.DecodeBytes(extra, &ver); err != nil {
						return err
					}
				}
				if ver > n-1024 {
					str := p.db.NewTrie(state.StorageTrieName(addr), thor.BytesToBytes32(acc.StorageRoot), ver)
					dn, err := p.cleanTrie(str, lastN)
					if err != nil {
						return err
					}
					deleteSCount += dn
				}
			}
			return nil
		})

		sAddr = thor.Address{}
		if err := p.saveState("s", sAddr); err != nil {
			return err
		}
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}
	}
}

func (p *Pruner) iterateStorage(start thor.Address, cb func(addr thor.Address) error) error {
	s := p.db.NewBucket([]byte{0, 's'})
	var rng = kv.Range{
		Start: start[:],
	}
	for {
		var addr *thor.Address
		if err := s.Iterate(rng, func(pair kv.Pair) bool {
			a := thor.BytesToAddress(pair.Key()[0:20])
			addr = &a
			return false
		}); err != nil {
			return err
		}
		if addr == nil {
			return nil
		}

		if err := cb(*addr); err != nil {
			return err
		}
		rng.Start = util.BytesPrefix(addr[:]).Limit
	}
}

// func (p *Pruner) pruneStorageTrie() error {
// 	deleteCount := 0
// 	var s struct {
// 		N    uint32
// 		Addr thor.Address
// 	}
// 	p.loadState("s", &s)
// 	go func() {
// 		for {
// 			<-time.After(15 * time.Second)
// 			log.Info("=====StorageTrie", "n", s.N, "delete", deleteCount)
// 		}
// 	}()

// 	for {
// 		n := s.N + 1024
// 		if err := p.waitUntil(n + thor.MaxStateHistory + 128); err != nil {
// 			return err
// 		}

// 		header, err := p.repo.NewBestChain().GetBlockHeader(n)
// 		if err != nil {
// 			return err
// 		}

// 		p.iterateStorage(s.Addr, func(addr thor.Address) error {
// 			s.Addr = addr
// 			if err := p.saveState("s", &s); err != nil {
// 				return err
// 			}
// 			accTrie := p.db.NewSecureTrie(state.AccountTrieName, header.StateRoot(), header.Number())
// 			data, extra, err := accTrie.GetExtra(addr[:])
// 			if err != nil {
// 				return err
// 			}
// 			if len(data) > 0 {
// 				var acc state.Account
// 				if err := rlp.DecodeBytes(data, &acc); err != nil {
// 					return err
// 				}

// 				var ver uint32
// 				if len(extra) > 0 {
// 					if err := rlp.DecodeBytes(extra, &ver); err != nil {
// 						return err
// 					}
// 				}
// 				if ver > s.N {
// 					// tr := p.db.NewTrie(state.StorageTrieName(addr), thor.BytesToBytes32(acc.StorageRoot), ver)
// 					// dn, err := p.cleanTrie(tr)
// 					// if err != nil {
// 					// 	return err
// 					// }
// 					// deleteCount += dn
// 					fmt.Println(addr)
// 				}
// 			}
// 			return nil
// 		})

// 		s.N = n
// 		s.Addr = thor.Address{}
// 		if err := p.saveState("s", &s); err != nil {
// 			return err
// 		}

// 		select {
// 		case <-p.ctx.Done():
// 			return p.ctx.Err()
// 		default:
// 		}
// 	}
// }

// func (p *Pruner) loop() error {
// 	var status status
// 	if err := status.Load(p.db); err != nil {
// 		return err
// 	}
// 	if status.Cycles == 0 && status.Step == stepInitiate {
// 		log.Info("pruner started")
// 	} else {
// 		log.Info("pruner started", "range", fmt.Sprintf("[%v, %v]", status.N1, status.N2), "step", status.Step)
// 	}

// 	pruner := p.db.NewTriePruner()

// 	if status.Cycles == 0 {
// 		if _, _, err := p.archiveIndexTrie(pruner, 0, 0); err != nil {
// 			return err
// 		}
// 		if _, _, _, _, err := p.archiveAccountTrie(pruner, 0, 0); err != nil {
// 			return err
// 		}
// 	}

// 	bestNum := func() uint32 {
// 		return p.repo.BestBlock().Header().Number()
// 	}

// 	waitUntil := func(n uint32) error {
// 		for {
// 			if bestNum() > n {
// 				return nil
// 			}
// 			select {
// 			case <-p.ctx.Done():
// 				return p.ctx.Err()
// 			case <-time.After(time.Second):
// 			}
// 		}
// 	}

// 	for {
// 		switch status.Step {
// 		case stepInitiate:
// 			// if err := pruner.SwitchLiveSpace(); err != nil {
// 			// 	return err
// 			// }
// 			status.N1 = status.N2
// 			status.N2 += 8192
// 			// not necessary to prune if n2 is too small
// 			// if status.N2 < thor.MaxStateHistory {
// 			// 	status.N2 = thor.MaxStateHistory
// 			// }
// 			if err := waitUntil(status.N2 + 128); err != nil {
// 				return err
// 			}
// 			log.Info("initiated", "range", fmt.Sprintf("[%v, %v]", status.N1, status.N2))
// 			status.Step = stepArchiveIndexTrie
// 		case stepArchiveIndexTrie:
// 			log.Info("archiving index trie...")
// 			nodeCount, entryCount, err := p.archiveIndexTrie(pruner, status.N1, status.N2)
// 			if err != nil {
// 				return err
// 			}
// 			log.Info("archived index trie", "nodes", nodeCount, "entries", entryCount)
// 			status.Step = stepArchiveAccountTrie
// 		case stepArchiveAccountTrie:
// 			log.Info("archiving account trie...")
// 			nodeCount, entryCount, sNodeCount, sEntryCount, err := p.archiveAccountTrie(pruner, status.N1, status.N2)
// 			if err != nil {
// 				return err
// 			}
// 			log.Info("archived account trie",
// 				"nodes", nodeCount, "entries", entryCount,
// 				"storageNodes", sNodeCount, "storageEntries", sEntryCount)
// 			status.Step = stepDropStale
// 		case stepDropStale:
// 			if err := waitUntil(status.N2 + thor.MaxStateHistory + 128); err != nil {
// 				return err
// 			}
// 			log.Info("sweeping stale nodes...")
// 			count, err := pruner.DropStaleNodes(p.ctx, status.N2)
// 			if err != nil {
// 				return err
// 			}
// 			log.Info("swept stale nodes", "count", count)

// 			status.Cycles++
// 			status.Step = stepInitiate
// 		default:
// 			return fmt.Errorf("unexpected pruner step: %v", status.Step)
// 		}

// 		if err := status.Save(p.db); err != nil {
// 			return err
// 		}
// 	}
// }

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

// func (p *Pruner) archiveIndexTrie(pruner *muxdb.TriePruner, n1, n2 uint32) (nodeCount, entryCount int, err error) {
// 	var (
// 		bestChain              = p.repo.NewBestChain()
// 		id1, id2, root1, root2 thor.Bytes32
// 		s1, s2                 *chain.BlockSummary
// 	)

// 	if id1, err = bestChain.GetBlockID(n1); err != nil {
// 		return
// 	}
// 	if s1, err = p.repo.GetBlockSummary(id1); err != nil {
// 		return
// 	}
// 	root1 = s1.IndexRoot

// 	if id2, err = bestChain.GetBlockID(n2); err != nil {
// 		return
// 	}
// 	if s2, err = p.repo.GetBlockSummary(id2); err != nil {
// 		return
// 	}
// 	root2 = s2.IndexRoot

// 	if n1 == 0 && n2 == 0 {
// 		root1 = thor.Bytes32{}
// 	}
// 	t1 := p.db.NewTrie(chain.IndexTrieName, root1, n1)
// 	t2 := p.db.NewTrie(chain.IndexTrieName, root2, n2)
// 	nodeCount, entryCount, err = pruner.ArchiveNodes(p.ctx, t1, t2, nil)
// 	return
// }

// func (p *Pruner) archiveAccountTrie(pruner *muxdb.TriePruner, n1, n2 uint32) (nodeCount, entryCount, storageNodeCount, storageEntryCount int, err error) {
// 	var (
// 		bestChain        = p.repo.NewBestChain()
// 		header1, header2 *block.Header
// 		root1, root2     thor.Bytes32
// 	)

// 	if header1, err = bestChain.GetBlockHeader(n1); err != nil {
// 		return
// 	}
// 	if header2, err = bestChain.GetBlockHeader(n2); err != nil {
// 		return
// 	}
// 	root1, root2 = header1.StateRoot(), header2.StateRoot()
// 	if n1 == 0 && n2 == 0 {
// 		root1 = thor.Bytes32{}
// 	}

// 	t1 := p.db.NewTrie(state.AccountTrieName, root1, n1)
// 	t2 := p.db.NewTrie(state.AccountTrieName, root2, n2)

// 	type sss struct {
// 		n      string
// 		r1, r2 thor.Bytes32
// 		v1, v2 uint32
// 	}

// 	var ss []*sss

// 	nodeCount, entryCount, err = pruner.ArchiveNodes(p.ctx, t1, t2, func(key, blob1, extra1, blob2, extra2 []byte) error {
// 		var sRoot1, sRoot2 thor.Bytes32
// 		var sv1, sv2 uint32
// 		if len(blob1) > 0 {
// 			var acc state.Account
// 			if err := rlp.DecodeBytes(blob1, &acc); err != nil {
// 				return err
// 			}
// 			sRoot1 = thor.BytesToBytes32(acc.StorageRoot)
// 			if len(extra1) > 0 {
// 				rlp.DecodeBytes(extra1, &sv1)
// 			}
// 		}
// 		if len(blob2) > 0 {
// 			var acc state.Account
// 			if err := rlp.DecodeBytes(blob2, &acc); err != nil {
// 				return err
// 			}
// 			sRoot2 = thor.BytesToBytes32(acc.StorageRoot)
// 			if len(extra2) > 0 {
// 				rlp.DecodeBytes(extra2, &sv2)
// 			}
// 		}
// 		if !sRoot2.IsZero() {
// 			ss = append(ss, &sss{
// 				n:  state.StorageTrieName(thor.BytesToBytes32(key)),
// 				r1: sRoot1,
// 				r2: sRoot2,
// 				v1: sv1,
// 				v2: sv2,
// 			})
// 		}
// 		// }
// 		return nil
// 	})

// 	for _, s := range ss {
// 		// fmt.Println(sRoot1, sv1)
// 		// fmt.Println(sRoot2, sv2)
// 		t1 := p.db.NewTrie(s.n, s.r1, s.v1)
// 		t2 := p.db.NewTrie(s.n, s.r2, s.v2)
// 		var n, e int
// 		n, e, err = pruner.ArchiveNodes(p.ctx, t1, t2, nil)
// 		if err != nil {
// 			return
// 		}
// 		storageNodeCount += n
// 		storageEntryCount += e
// 	}
// 	return
// }
