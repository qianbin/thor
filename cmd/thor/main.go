// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/inconshreveable/log15"
	isatty "github.com/mattn/go-isatty"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vechain/thor/api"
	"github.com/vechain/thor/block"
	"github.com/vechain/thor/chain"
	"github.com/vechain/thor/cmd/thor/node"
	"github.com/vechain/thor/cmd/thor/solo"
	"github.com/vechain/thor/genesis"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/logdb"
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/state"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
	"github.com/vechain/thor/txpool"
	"gopkg.in/cheggaaa/pb.v1"
	cli "gopkg.in/urfave/cli.v1"
)

var (
	version   string
	gitCommit string
	gitTag    string
	log       = log15.New()

	defaultTxPoolOptions = txpool.Options{
		Limit:           10000,
		LimitPerAccount: 16,
		MaxLifetime:     20 * time.Minute,
	}
)

func fullVersion() string {
	versionMeta := "release"
	if gitTag == "" {
		versionMeta = "dev"
	}
	return fmt.Sprintf("%s-%s-%s", version, gitCommit, versionMeta)
}

func main() {
	app := cli.App{
		Version:   fullVersion(),
		Name:      "Thor",
		Usage:     "Node of VeChain Thor Network",
		Copyright: "2018 VeChain Foundation <https://vechain.org/>",
		Flags: []cli.Flag{
			networkFlag,
			configDirFlag,
			dataDirFlag,
			cacheFlag,
			beneficiaryFlag,
			targetGasLimitFlag,
			apiAddrFlag,
			apiCorsFlag,
			apiTimeoutFlag,
			apiCallGasLimitFlag,
			apiBacktraceLimitFlag,
			verbosityFlag,
			maxPeersFlag,
			p2pPortFlag,
			natFlag,
			bootNodeFlag,
			skipLogsFlag,
			pprofFlag,
			verifyLogsFlag,
		},
		Action: defaultAction,
		Commands: []cli.Command{
			{
				Name:  "solo",
				Usage: "client runs in solo mode for test & dev",
				Flags: []cli.Flag{
					dataDirFlag,
					apiAddrFlag,
					apiCorsFlag,
					apiTimeoutFlag,
					apiCallGasLimitFlag,
					apiBacktraceLimitFlag,
					onDemandFlag,
					persistFlag,
					gasLimitFlag,
					verbosityFlag,
					pprofFlag,
					verifyLogsFlag,
				},
				Action: soloAction,
			},
			{
				Name:  "master-key",
				Usage: "master key management",
				Flags: []cli.Flag{
					configDirFlag,
					importMasterKeyFlag,
					exportMasterKeyFlag,
				},
				Action: masterKeyAction,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type pruneStatus struct {
	N uint32
	I uint32
}

func getPruneStatus(s kv.Store) *pruneStatus {
	val, _ := s.Get([]byte("ps"))

	var ps pruneStatus
	rlp.DecodeBytes(val, &ps)
	return &ps
}

func setPruneStatus(s kv.Store, ps *pruneStatus) {
	data, _ := rlp.EncodeToBytes(ps)
	s.Put([]byte("ps"), data)
}

func makeHashKey(hash []byte, path []byte) []byte {
	var key [40]byte
	for i := 0; i < len(path) && i < 16; i++ {
		if i%2 == 0 {
			key[i/2] |= path[i] << 4
		} else {
			key[i/2] |= path[i]
		}
	}
	copy(key[8:], hash)
	return key[:]
}
func prune(db *muxdb.MuxDB, chain *chain.Chain) error {
	lowStore := db.LowStore()

	go func() {
		var (
			n1       = uint32(0)
			n2       = uint32(0)
			rollingI = 0
			bloom    = muxdb.NewBloom(256*1024*1024*8, 3)
		)

		type op struct {
			del  bool
			k, v []byte
		}

		var ops []op
		flushBatch := func() error {
			if len(ops) > 0 {
				return lowStore.Batch(func(p kv.Putter) error {
					for _, op := range ops {
						if op.del {
							p.Delete(op.k)
						} else {
							p.Put(op.k, op.v)
						}
					}
					ops = nil
					return nil
				})
			}
			return nil
		}

		batchPut := func(k, v []byte) error {
			ops = append(ops, op{
				k: append([]byte(nil), k...),
				v: append([]byte(nil), v...),
			})
			if len(ops) > 8192 {
				return flushBatch()
			}
			return nil
		}

		batchDel := func(k []byte) error {
			ops = append(ops, op{
				del: true,
				k:   append([]byte(nil), k...),
			})
			if len(ops) >= 8192 {
				return flushBatch()
			}
			return nil
		}

		for {
			n1 = n2

			for {
				if chain.BestBlock().Header().Number() > n1+65536 {
					break
				}
				time.Sleep(time.Second)
			}

			rollingI++
			newPrefix, oldPrefix, maturePrefix := db.RollTrie(rollingI)

			fmt.Printf("Pruner: rolling to prefix %x %x %x\n", newPrefix, oldPrefix, maturePrefix)
			n2 = chain.BestBlock().Header().Number() + 20

			for {
				if chain.BestBlock().Header().Number() > n2+20 {
					break
				}
				time.Sleep(time.Second)
			}

			fmt.Printf("Pruner: start [%v, %v]\n", n1, n2)
			indexEntries := 0
			accEntries := 0
			storageEntries := 0

			id1, err := chain.NewTrunk().GetBlockID(n1)
			if err != nil {
				panic(err)
			}
			id2, err := chain.NewTrunk().GetBlockID(n2)
			if err != nil {
				panic(err)
			}

			h1, iroot1, err := chain.GetBlockHeader(id1)
			if err != nil {
				panic(err)
			}

			h2, iroot2, err := chain.GetBlockHeader(id2)
			if err != nil {
				panic(err)
			}
			if n1 == 0 {
				iroot1 = thor.Bytes32{}
			}

			itr1 := db.NewTrieNoFillCache("i", iroot1, false)
			itr2 := db.NewTrieNoFillCache("i", iroot2, false)

			iit1, err := itr1.NodeIterator(nil)
			if err != nil {
				panic(err)
			}
			iit2, err := itr2.NodeIterator(nil)
			if err != nil {
				panic(err)
			}

			idiff, _ := trie.NewDifferenceIterator(iit1, iit2)
			for idiff.Next(true) {
				if h := idiff.Hash(); !h.IsZero() {
					data, err := idiff.Node()
					if err != nil {
						panic(err)
					}
					indexEntries++

					bloom.Add(h)
					if err := batchPut(append(append(maturePrefix, 'i'), makeHashKey(h[:], idiff.Path())...), data); err != nil {
						panic(err)
					}
				}
			}

			fmt.Println("Pruner: indexTrie entries ", indexEntries)

			stateRoot1 := h1.StateRoot()
			stateRoot2 := h2.StateRoot()

			if n1 == 0 {
				stateRoot1 = thor.Bytes32{}
			}

			tr1 := db.NewTrieNoFillCache("a", stateRoot1, false)
			tr2 := db.NewTrieNoFillCache("a", stateRoot2, false)

			it1, err := tr1.NodeIterator(nil)
			if err != nil {
				panic(err)
			}
			it2, err := tr2.NodeIterator(nil)
			if err != nil {
				panic(err)
			}

			diff, _ := trie.NewDifferenceIterator(it1, it2)
			for diff.Next(true) {
				if h := diff.Hash(); !h.IsZero() {
					data, err := diff.Node()
					if err != nil {
						panic(err)
					}
					accEntries++
					bloom.Add(h)
					if err := batchPut(append(append(maturePrefix, 'a'), makeHashKey(h[:], diff.Path())...), data); err != nil {
						panic(err)
					}
				}

				if diff.Leaf() {
					blob2 := diff.LeafBlob()
					var acc2 state.Account
					if err := rlp.DecodeBytes(blob2, &acc2); err != nil {
						panic(err)
					}

					if len(acc2.StorageRoot) > 0 {
						sroot2 := thor.BytesToBytes32(acc2.StorageRoot)
						blob1, err := tr1.Get(diff.LeafKey())
						if err != nil {
							panic(err)
						}
						var sroot1 thor.Bytes32
						if len(blob1) > 0 {
							var acc1 state.Account
							if err := rlp.DecodeBytes(blob1, &acc1); err != nil {
								panic(err)
							}
							sroot1 = thor.BytesToBytes32(acc1.StorageRoot)
						}
						sprefix := []byte("s" + string(diff.LeafKey()))

						sit1, err := db.NewTrieNoFillCache(string(sprefix), sroot1, false).NodeIterator(nil)
						if err != nil {
							panic(err)
						}
						sit2, err := db.NewTrieNoFillCache(string(sprefix), sroot2, false).NodeIterator(nil)
						if err != nil {
							panic(err)
						}
						sit, _ := trie.NewDifferenceIterator(sit1, sit2)
						for sit.Next(true) {
							if h := sit.Hash(); !h.IsZero() {
								n, err := sit.Node()
								if err != nil {
									panic(err)
								}
								storageEntries++
								bloom.Add(h)
								if err := batchPut(append(append(maturePrefix, sprefix...), makeHashKey(h[:], sit.Path())...), n); err != nil {
									panic(err)
								}
							}
						}
						if err := sit.Error(); err != nil {
							panic(err)
						}
					}
				}
			}
			flushBatch()
			fmt.Println("Pruner: account trie entries ", accEntries)
			fmt.Println("Pruner: storage trie entries ", storageEntries)

			for {
				if chain.BestBlock().Header().Number() > n2+65536+20 {
					break
				}
				time.Sleep(time.Second)
			}

			fmt.Println("Pruner: deleting...")

			deleted := 0

			r := func() kv.Range {
				rr := util.BytesPrefix(oldPrefix)
				return kv.Range{
					From: rr.Start,
					To:   rr.Limit,
				}
			}()

			for {
				counter := 0
				if err := lowStore.Iterate(r, func(key []byte, val []byte) bool {
					if counter >= 8192 {
						r.From = key
						return false
					}

					deleted++
					h := thor.BytesToBytes32(key)
					if !bloom.Test(h) {
						db.EvictTrieCache(h[:])
					}
					if err := batchDel(key); err != nil {
						panic(err)
					}
					counter++

					return true
				}); err != nil {
					panic(err)
				}
				if counter == 0 {
					break
				}
			}

			flushBatch()
			fmt.Println("Pruner: delete entries:", deleted, ", mature:", indexEntries, accEntries, storageEntries, ", ratio:", float64((indexEntries+accEntries+storageEntries)*100)/float64(deleted), "%")

			// fmt.Println("Pruner: Compacting...")
			// rg := util.BytesPrefix(oldPrefix)
			// lowStore.Compact(rg.Start, rg.Limit)
			// fmt.Println("Pruner: Compact done")

		}
	}()

	// go func() {
	// 	// prune index trie
	// 	var (
	// 		bloom = thor.NewBigBloom(64, 3)
	// 		gen   = uint32(0)
	// 	)

	// 	v, _ := configStore.Get([]byte("index-trie-pruned"))
	// 	if len(v) == 4 {
	// 		gen = binary.BigEndian.Uint32(v)
	// 	}

	// 	for {
	// 		bloom.Reset()
	// 		for {
	// 			if chain.BestBlock().Header().Number() >= (gen+1)<<16+50 {
	// 				break
	// 			}
	// 			time.Sleep(time.Second)
	// 		}
	// 		n1 := gen << 16
	// 		n2 := (gen + 1) << 16
	// 		gen++

	// 		fmt.Printf("I Pruner: start [%v, %v]\n", n1, n2)

	// 		// index trie

	// 		id1, err := chain.NewTrunk().GetBlockID(n1)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		id2, err := chain.NewTrunk().GetBlockID(n2)
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		_, root1, err := chain.GetBlockHeader(id1)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		_, root2, err := chain.GetBlockHeader(id2)
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		indexTrieEntries := 0
	// 		t1 := db.NewTrie("i", root1, n1, false)
	// 		it1, err := t1.NodeIterator(nil)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		it2, err := db.NewTrie("i", root2, n2, false).NodeIterator(nil)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		it, _ := trie.NewDifferenceIterator(it1, it2)
	// 		for it.Next(true) {
	// 			if h := it.Hash(); !h.IsZero() {
	// 				indexTrieEntries++
	// 				bloom.Add(h)
	// 			}
	// 		}
	// 		if err := it.Error(); err != nil {
	// 			panic(err)
	// 		}
	// 		fmt.Println("I Pruner: index trie entries", indexTrieEntries)

	// 		prefix := t1.Prefix()

	// 		fmt.Printf("I Pruner: deleting prefix %x...\n", prefix)
	// 		scaned := 0
	// 		deleted := 0

	// 		prefixLen := len(prefix)

	// 		lowStore := db.LowStore()
	// 		var tempKey [2 + 2 + 32]byte

	// 		lowStore.Iterate(prefix, func(key, val []byte) error {
	// 			scaned++
	// 			lowStore.Batch(func(w kv.Putter) error {
	// 				if bloom.Test(thor.BytesToBytes32(key[prefixLen:])) {
	// 					copy(tempKey[:], key)
	// 					tempKey[2] = 255
	// 					tempKey[3] = 255
	// 					w.Put(tempKey[:], val)
	// 				} else {
	// 					deleted++
	// 				}
	// 				w.Delete(key)
	// 				return nil
	// 			})
	// 			return nil
	// 		})

	// 		fmt.Println("I Pruner: deleted", deleted, "/", scaned, "entries  ", float64(deleted*100)/float64(scaned), "%")
	// 		mu.Lock()
	// 		fmt.Println("I Pruner: do compact")

	// 		if err := lowStore.Compact(prefix); err != nil {
	// 			fmt.Println(err)
	// 		}

	// 		fmt.Println("I Pruner: compact done")
	// 		mu.Unlock()
	// 		var kk [4]byte
	// 		binary.BigEndian.PutUint32(kk[:], gen)
	// 		configStore.Put([]byte("index-trie-pruned"), kk[:])
	// 	}
	// }()
	// go func() {
	// 	var (
	// 		gen    = uint32(0)
	// 		ibloom = thor.NewBigBloom(64, 3)
	// 		abloom = thor.NewBigBloom(64, 3)
	// 		sbloom = thor.NewBigBloom(64, 3)
	// 	)

	// 	v, _ := configStore.Get([]byte("state-pruned"))
	// 	if len(v) == 4 {
	// 		gen = binary.BigEndian.Uint32(v)
	// 	}

	// 	for {
	// 		ibloom.Reset()
	// 		abloom.Reset()
	// 		sbloom.Reset()
	// 		for {
	// 			if chain.BestBlock().Header().Number() >= (gen+1)<<16+50 {
	// 				break
	// 			}
	// 			time.Sleep(time.Second)
	// 		}
	// 		n1 := gen << 16
	// 		n2 := (gen + 1) << 16
	// 		gen++

	// 		fmt.Printf("Pruner: start [%v, %v]\n", n1, n2)

	// 		id1, err := chain.NewTrunk().GetBlockID(n1)
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		id2, err := chain.NewTrunk().GetBlockID(n2)
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		h1, iroot1, err := chain.GetBlockHeader(id1)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		h2, iroot2, err := chain.GetBlockHeader(id2)
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		{
	// 			indexTrieEntries := 0
	// 			t1 := db.NewTrie("i", iroot1, n1, false)
	// 			it1, err := t1.NodeIterator(nil)
	// 			if err != nil {
	// 				panic(err)
	// 			}
	// 			it2, err := db.NewTrie("i", iroot2, n2, false).NodeIterator(nil)
	// 			if err != nil {
	// 				panic(err)
	// 			}
	// 			it, _ := trie.NewDifferenceIterator(it1, it2)
	// 			for it.Next(true) {
	// 				if h := it.Hash(); !h.IsZero() {
	// 					indexTrieEntries++
	// 					ibloom.Add(h)
	// 				}
	// 			}
	// 			if err := it.Error(); err != nil {
	// 				panic(err)
	// 			}
	// 			fmt.Println("Pruner: index trie entries", indexTrieEntries)
	// 			prefix := t1.Prefix()

	// 			fmt.Printf("Pruner: deleting prefix %x...\n", prefix)
	// 			scaned := 0
	// 			deleted := 0

	// 			prefixLen := len(prefix)

	// 			lowStore := db.LowStore()
	// 			var tempKey [2 + 2 + 32]byte

	// 			lowStore.Iterate(prefix, func(key, val []byte) error {
	// 				scaned++
	// 				lowStore.Batch(func(w kv.Putter) error {
	// 					if ibloom.Test(thor.BytesToBytes32(key[prefixLen:])) {
	// 						copy(tempKey[:], key)
	// 						tempKey[1] = 255
	// 						tempKey[2] = 255
	// 						w.Put(tempKey[:], val)
	// 					} else {
	// 						deleted++
	// 					}
	// 					w.Delete(key)
	// 					return nil
	// 				})
	// 				return nil
	// 			})

	// 			fmt.Println("Pruner: deleted", deleted, "/", scaned, "entries  ", float64(deleted*100)/float64(scaned), "%")
	// 		}

	// 		accountTrieEntries := 0
	// 		storageTrieEntries := 0

	// 		tr1 := db.NewTrie("a", h1.StateRoot(), n1, false)
	// 		it1, err := tr1.NodeIterator(nil)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		tr2 := db.NewTrie("a", h2.StateRoot(), n2, false)
	// 		it2, err := tr2.NodeIterator(nil)
	// 		if err != nil {
	// 			panic(err)
	// 		}

	// 		it, _ := trie.NewDifferenceIterator(it1, it2)
	// 		for it.Next(true) {
	// 			if h := it.Hash(); !h.IsZero() {
	// 				abloom.Add(h)
	// 				accountTrieEntries++
	// 			}
	// 			if it.Leaf() {
	// 				blob2 := it.LeafBlob()
	// 				var acc2 state.Account
	// 				if err := rlp.DecodeBytes(blob2, &acc2); err != nil {
	// 					panic(err)
	// 				}

	// 				if len(acc2.StorageRoot) > 0 {
	// 					sroot2 := thor.BytesToBytes32(acc2.StorageRoot)
	// 					blob1, err := tr1.Get(it.LeafKey())
	// 					if err != nil {
	// 						panic(err)
	// 					}
	// 					var sroot1 thor.Bytes32
	// 					if len(blob1) > 0 {
	// 						var acc1 state.Account
	// 						if err := rlp.DecodeBytes(blob1, &acc1); err != nil {
	// 							panic(err)
	// 						}
	// 						sroot1 = thor.BytesToBytes32(acc1.StorageRoot)
	// 					}
	// 					sit1, err := db.NewTrie("s", sroot1, n1, false).NodeIterator(nil)
	// 					if err != nil {
	// 						panic(err)
	// 					}
	// 					sit2, err := db.NewTrie("s", sroot2, n2, false).NodeIterator(nil)
	// 					if err != nil {
	// 						panic(err)
	// 					}
	// 					sit, _ := trie.NewDifferenceIterator(sit1, sit2)
	// 					for sit.Next(true) {
	// 						if h := sit.Hash(); !h.IsZero() {
	// 							storageTrieEntries++
	// 							sbloom.Add(h)
	// 						}
	// 					}
	// 					if err := sit.Error(); err != nil {
	// 						panic(err)
	// 					}
	// 				}
	// 			}
	// 		}
	// 		if err := it.Error(); err != nil {
	// 			panic(err)
	// 		}
	// 		fmt.Println("Pruner: account trie entries", accountTrieEntries)
	// 		fmt.Println("Pruner: storage trie entries", storageTrieEntries)

	// 		lowStore := db.LowStore()

	// 		sprefix := db.NewTrie("s", thor.Bytes32{}, n1, false).Prefix()
	// 		sprefixLen := len(sprefix)
	// 		var tempKey [2 + 2 + 32]byte
	// 		fmt.Printf("Pruner: deleting prefix %x...\n", sprefix)
	// 		sscaned := 0
	// 		sdeleted := 0
	// 		lowStore.Iterate(sprefix, func(key, val []byte) error {
	// 			sscaned++
	// 			lowStore.Batch(func(w kv.Putter) error {
	// 				if sbloom.Test(thor.BytesToBytes32(key[sprefixLen:])) {
	// 					copy(tempKey[:], key)
	// 					tempKey[1] = 255
	// 					tempKey[2] = 255
	// 					w.Put(tempKey[:], val)
	// 				} else {
	// 					sdeleted++
	// 				}
	// 				w.Delete(key)
	// 				return nil
	// 			})
	// 			return nil
	// 		})

	// 		fmt.Println("Pruner: storage deleted", sdeleted, "/", sscaned, "entries  ", float64(sdeleted*100)/float64(sscaned), "%")

	// 		for {
	// 			if chain.BestBlock().Header().Number() >= (gen)<<16+65536+50 {
	// 				break
	// 			}
	// 			time.Sleep(time.Second)
	// 		}

	// 		aprefix := db.NewTrie("a", thor.Bytes32{}, n1, false).Prefix()

	// 		fmt.Printf("Pruner: deleting prefix %x...\n", aprefix)
	// 		ascaned := 0
	// 		adeleted := 0

	// 		aprefixLen := len(aprefix)

	// 		lowStore.Iterate(aprefix, func(key, val []byte) error {
	// 			ascaned++
	// 			lowStore.Batch(func(w kv.Putter) error {
	// 				if abloom.Test(thor.BytesToBytes32(key[aprefixLen:])) {
	// 					copy(tempKey[:], key)
	// 					tempKey[1] = 255
	// 					tempKey[2] = 255
	// 					w.Put(tempKey[:], val)
	// 				} else {
	// 					adeleted++
	// 				}
	// 				w.Delete(key)
	// 				return nil
	// 			})
	// 			return nil
	// 		})

	// 		fmt.Println("Pruner: account deleted", adeleted, "/", ascaned, "entries  ", float64(adeleted*100)/float64(ascaned), "%")

	// 		// fmt.Println("Pruner: do compact")

	// 		// if err := lowStore.Compact([]byte{aprefix[0], 0, 0}, db.NewTrie("a", thor.Bytes32{}, n2, false).Prefix()[:3]); err != nil {
	// 		// 	fmt.Println(err)
	// 		// }
	// 		// fmt.Println("Pruner: compact done")

	// 		var kk [4]byte
	// 		binary.BigEndian.PutUint32(kk[:], gen)
	// 		configStore.Put([]byte("state-pruned"), kk[:])
	// 	}
	// }()

	return nil
}

func defaultAction(ctx *cli.Context) error {
	exitSignal := handleExitSignal()

	defer func() { log.Info("exited") }()

	initLogger(ctx)
	gene, forkConfig := selectGenesis(ctx)
	instanceDir := makeInstanceDir(ctx, gene)

	mainDB := openMainDB(ctx, instanceDir)
	defer func() { log.Info("closing main database..."); mainDB.Close() }()

	// f := func(p byte) {
	// 	n := 0
	// 	size := 0
	// 	it := stateDB.NewIterator(*kv.NewRangeWithBytesPrefix([]byte{p}))
	// 	for it.Next() {
	// 		n++
	// 		size += len(it.Value())
	// 	}
	// 	it.Release()
	// 	fmt.Println(n, size)
	// }
	// f(0)
	// f(1)
	// f(2)
	// f(3)

	// lowStore := mainDB.LowStore()
	// p1, p2, _ := mainDB.RollTrie(0)
	// lowStore.Compact(util.BytesPrefix(p1).Start, util.BytesPrefix(p1).Limit)
	// fmt.Println("1")
	// lowStore.Compact(util.BytesPrefix(p2).Start, util.BytesPrefix(p2).Limit)
	// return nil

	skipLogs := ctx.Bool(skipLogsFlag.Name)

	logDB := openLogDB(ctx, instanceDir)
	defer func() { log.Info("closing log database..."); logDB.Close() }()

	chain := initChain(gene, mainDB, logDB)

	// n := chain.BestBlock().Header().Number()
	// b := chain.NewTrunk()

	// for i := uint32(0); i < n; i++ {
	// 	_, err := b.GetBlockID(i)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	if i%1000 == 0 {
	// 		fmt.Println(i)
	// 	}

	// }

	// cons := consensus.New(chain, mainDB, forkConfig)
	// for i := uint32(2165914); i < chain.BestBlock().Header().Number(); i++ {
	// 	blk, err := chain.NewTrunk().GetBlock(i)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	_, _, err = cons.Process(blk, blk.Header().Timestamp())
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	// return nil

	// go func() {
	// 	defer profile.Start().Stop()
	// 	time.Sleep(time.Minute)
	// }()
	// cons := consensus.New(chain, mainDB, forkConfig)

	// for i := uint32(0); i < 10000; i++ {
	// 	blk, err := chain.NewTrunk().GetBlock(3401001)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	_, _, err = cons.Process(blk, blk.Header().Timestamp())
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	// slow #2407590…2d9049c5 low gps
	// profile 3189578 3363312 3358737 3363306

	// b := chain.NewBranch(chain.BestBlock().Header().ID())
	// n := chain.BestBlock().Header().Number()

	// id, _ := b.GetBlockID(n)
	// _, root, _ := chain.GetBlockHeader(id)

	// it, _ := mainDB.NewTrie("", root, n, false).NodeIterator(nil)
	// max := 0

	// for it.Next(true) {
	// 	if h := it.Hash(); !h.IsZero() {
	// 		l := len(it.Path())
	// 		if l > max {
	// 			max = l
	// 			fmt.Println(max)
	// 		}

	// 	}
	// }

	// for i := uint32(0); i < n; i++ {
	// 	_, err := b.GetBlockID(i)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	if i%1000 == 0 {
	// 		fmt.Println(i)
	// 	}
	// }
	// return nil

	// _, indexRoot, _ := chain.GetBlockHeader(chain.BestBlock().Header().ID())

	// it, _ := triex.NewTrie(indexRoot, false).NodeIterator(nil)
	// n := 0

	// for it.Next(true) {
	// 	if h := it.Hash(); !h.IsZero() {
	// 		// fmt.Println(h)
	// 		n++
	// 	}
	// }

	// fmt.Println(n)
	// return nil
	// prune(triex, stateDB, chain)

	// tr := chain.NewTrunk()

	// var blocks []*block.Block
	// for i := 3401831; i < 3401832; i++ {
	// 	id, _ := tr.GetBlockID(uint32(i))
	// 	b, _ := chain.GetBlock(id)
	// 	blocks = append(blocks, b)
	// }
	// fmt.Println(len(blocks))

	// con := consensus.New(chain, mainDB, forkConfig)
	// for _, b := range blocks {
	// 	_, _, err := con.Process(b, b.Header().Timestamp())
	// 	fmt.Println(err)
	// }

	// defer profile.Start().Stop()

	// for i := 0; i < 1; i++ {
	// 	for _, b := range blocks {
	// 		con.Process(b, b.Header().Timestamp())
	// 	}
	// }

	// return nil
	// tr := mainDB.NewTrie("", chain.BestBlock().Header().StateRoot(), true)

	// data, _ := tr.Get(thor.MustParseAddress("0x828cA60C9D6Dd6266249588dBE00a67dF83d5D4D").Bytes())
	// var acc state.Account
	// rlp.DecodeBytes(data, &acc)

	// tt := mainDB.NewTrie("", thor.BytesToBytes32(acc.StorageRoot), false)
	// it, _ := tt.NodeIterator(nil)

	// n := 0
	// m := make(map[int]int)
	// for it.Next(true) {
	// 	if !it.Hash().IsZero() {
	// 		n++
	// 		m[len(it.Path())]++
	// 	}

	// }
	// fmt.Println(n)
	// fmt.Println(m)
	// return nil

	prune(mainDB, chain)

	master := loadNodeMaster(ctx)

	printStartupMessage1(gene, chain, master, instanceDir, forkConfig)

	if !skipLogs {
		if err := syncLogDB(exitSignal, chain, logDB, ctx.Bool(verifyLogsFlag.Name)); err != nil {
			return err
		}
	}

	txPool := txpool.New(chain, mainDB, defaultTxPoolOptions)
	defer func() { log.Info("closing tx pool..."); txPool.Close() }()

	p2pcom := newP2PComm(ctx, chain, txPool, instanceDir)
	apiHandler, apiCloser := api.New(
		chain,
		mainDB,
		txPool,
		logDB,
		p2pcom.comm,
		ctx.String(apiCorsFlag.Name),
		uint32(ctx.Int(apiBacktraceLimitFlag.Name)),
		uint64(ctx.Int(apiCallGasLimitFlag.Name)),
		ctx.Bool(pprofFlag.Name),
		skipLogs,
		forkConfig)
	defer func() { log.Info("closing API..."); apiCloser() }()

	apiURL, srvCloser := startAPIServer(ctx, apiHandler, chain.GenesisBlock().Header().ID())
	defer func() { log.Info("stopping API server..."); srvCloser() }()

	printStartupMessage2(apiURL, getNodeID(ctx))

	p2pcom.Start()
	defer p2pcom.Stop()

	return node.New(
		master,
		chain,
		mainDB,
		logDB,
		txPool,
		filepath.Join(instanceDir, "tx.stash"),
		p2pcom.comm,
		uint64(ctx.Int(targetGasLimitFlag.Name)),
		skipLogs,
		forkConfig).
		Run(exitSignal)
}

func soloAction(ctx *cli.Context) error {
	exitSignal := handleExitSignal()
	defer func() { log.Info("exited") }()

	initLogger(ctx)
	gene := genesis.NewDevnet()
	// Solo forks from the start
	forkConfig := thor.ForkConfig{}

	var (
		mainDB      *muxdb.MuxDB
		logDB       *logdb.LogDB
		instanceDir string
	)

	if ctx.Bool("persist") {
		instanceDir = makeInstanceDir(ctx, gene)
		mainDB = openMainDB(ctx, instanceDir)
		logDB = openLogDB(ctx, instanceDir)
	} else {
		instanceDir = "Memory"
		mainDB = openMemDB()
		logDB = openMemLogDB()
	}

	defer func() { log.Info("closing main database..."); mainDB.Close() }()
	defer func() { log.Info("closing log database..."); logDB.Close() }()

	chain := initChain(gene, mainDB, logDB)
	if err := syncLogDB(exitSignal, chain, logDB, ctx.Bool(verifyLogsFlag.Name)); err != nil {
		return err
	}

	txPool := txpool.New(chain, mainDB, defaultTxPoolOptions)
	defer func() { log.Info("closing tx pool..."); txPool.Close() }()

	apiHandler, apiCloser := api.New(
		chain,
		mainDB,
		txPool,
		logDB,
		solo.Communicator{},
		ctx.String(apiCorsFlag.Name),
		uint32(ctx.Int(apiBacktraceLimitFlag.Name)),
		uint64(ctx.Int(apiCallGasLimitFlag.Name)),
		ctx.Bool(pprofFlag.Name),
		false,
		forkConfig)
	defer func() { log.Info("closing API..."); apiCloser() }()

	apiURL, srvCloser := startAPIServer(ctx, apiHandler, chain.GenesisBlock().Header().ID())
	defer func() { log.Info("stopping API server..."); srvCloser() }()

	printSoloStartupMessage(gene, chain, instanceDir, apiURL, forkConfig)

	return solo.New(chain,
		mainDB,
		logDB,
		txPool,
		uint64(ctx.Int("gas-limit")),
		ctx.Bool("on-demand"),
		forkConfig).Run(exitSignal)
}

func masterKeyAction(ctx *cli.Context) error {
	hasImportFlag := ctx.Bool(importMasterKeyFlag.Name)
	hasExportFlag := ctx.Bool(exportMasterKeyFlag.Name)
	if hasImportFlag && hasExportFlag {
		return fmt.Errorf("flag %s and %s are exclusive", importMasterKeyFlag.Name, exportMasterKeyFlag.Name)
	}

	if !hasImportFlag && !hasExportFlag {
		masterKey, err := loadOrGeneratePrivateKey(masterKeyPath(ctx))
		if err != nil {
			return err
		}
		fmt.Println("Master:", thor.Address(crypto.PubkeyToAddress(masterKey.PublicKey)))
		return nil
	}

	if hasImportFlag {
		if isatty.IsTerminal(os.Stdin.Fd()) {
			fmt.Println("Input JSON keystore (end with ^d):")
		}
		keyjson, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(keyjson, &map[string]interface{}{}); err != nil {
			return errors.WithMessage(err, "unmarshal")
		}
		password, err := readPasswordFromNewTTY("Enter passphrase: ")
		if err != nil {
			return err
		}

		key, err := keystore.DecryptKey(keyjson, password)
		if err != nil {
			return errors.WithMessage(err, "decrypt")
		}

		if err := crypto.SaveECDSA(masterKeyPath(ctx), key.PrivateKey); err != nil {
			return err
		}
		fmt.Println("Master key imported:", thor.Address(key.Address))
		return nil
	}

	if hasExportFlag {
		masterKey, err := loadOrGeneratePrivateKey(masterKeyPath(ctx))
		if err != nil {
			return err
		}

		password, err := readPasswordFromNewTTY("Enter passphrase: ")
		if err != nil {
			return err
		}
		if password == "" {
			return errors.New("non-empty passphrase required")
		}
		confirm, err := readPasswordFromNewTTY("Confirm passphrase: ")
		if err != nil {
			return err
		}

		if password != confirm {
			return errors.New("passphrase confirmation mismatch")
		}

		keyjson, err := keystore.EncryptKey(&keystore.Key{
			PrivateKey: masterKey,
			Address:    crypto.PubkeyToAddress(masterKey.PublicKey),
			Id:         uuid.NewRandom()},
			password, keystore.StandardScryptN, keystore.StandardScryptP)
		if err != nil {
			return err
		}
		if isatty.IsTerminal(os.Stdout.Fd()) {
			fmt.Println("=== JSON keystore ===")
		}
		_, err = fmt.Println(string(keyjson))
		return err
	}
	return nil
}

func seekLogDBSyncPosition(chain *chain.Chain, logDB *logdb.LogDB) (uint32, error) {
	best := chain.BestBlock().Header()
	if best.Number() == 0 {
		return 0, nil
	}

	newestID, err := logDB.NewestBlockID()
	if err != nil {
		return 0, err
	}

	if block.Number(newestID) == 0 {
		return 0, nil
	}

	if newestID == best.ID() {
		return best.Number(), nil
	}

	seekStart := block.Number(newestID)
	if seekStart >= best.Number() {
		seekStart = best.Number() - 1
	}

	header, err := chain.NewTrunk().GetBlockHeader(seekStart)
	if err != nil {
		return 0, err
	}

	for header.Number() > 0 {
		has, err := logDB.HasBlockID(header.ID())
		if err != nil {
			return 0, err
		}
		if has {
			break
		}

		header, _, err = chain.GetBlockHeader(header.ParentID())
		if err != nil {
			return 0, err
		}
	}
	return block.Number(header.ID()) + 1, nil

}

func syncLogDB(ctx context.Context, chain *chain.Chain, logDB *logdb.LogDB, verify bool) error {
	startPos, err := seekLogDBSyncPosition(chain, logDB)
	if err != nil {
		return errors.Wrap(err, "seek log db sync position")
	}
	if verify && startPos > 0 {
		if err := verifyLogDB(ctx, startPos-1, chain, logDB); err != nil {
			return errors.Wrap(err, "verify log db")
		}
	}

	bestNum := chain.BestBlock().Header().Number()

	if bestNum == startPos {
		return nil
	}

	if startPos == 0 {
		fmt.Println(">> Rebuilding log db <<")
		startPos = 1 // block 0 can be skipped
	} else {
		fmt.Println(">> Syncing log db <<")
	}

	pb := pb.New64(int64(bestNum)).
		Set64(int64(startPos - 1)).
		SetMaxWidth(90).
		Start()

	defer func() { pb.NotPrint = true }()

	it := chain.NewIterator(256).Seek(startPos)

	task := logDB.NewTask()
	taskLen := 0

	for it.Next() {
		b := it.Block()

		task.ForBlock(b.Header())
		txs := b.Transactions()
		if len(txs) > 0 {
			receipts, err := chain.GetReceipts(b.Header().ID())
			if err != nil {
				return errors.Wrap(err, "get block receipts")
			}

			for i, tx := range txs {
				origin, _ := tx.Origin()
				task.Write(tx.ID(), origin, receipts[i].Outputs)
				taskLen++
			}
		}
		if taskLen > 512 {
			if err := task.Commit(); err != nil {
				return errors.Wrap(err, "write logs")
			}
			task = logDB.NewTask()
			taskLen = 0
		}
		pb.Add64(1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if taskLen > 0 {
		if err := task.Commit(); err != nil {
			return errors.Wrap(err, "write logs")
		}
	}

	if err := it.Error(); err != nil {
		return errors.Wrap(err, "read block")
	}
	pb.Finish()
	return nil
}
