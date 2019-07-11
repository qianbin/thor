package main

import (
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/block"
	"github.com/vechain/thor/chain"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
	"github.com/vechain/thor/tx"
)

type blockAndReceipts struct {
	Block    *block.Block
	Receipts tx.Receipts
}

func exportChain(chain *chain.Chain, w io.Writer, endBlockNum uint32, progress func(uint32)) error {
	it := chain.NewIterator(chain.BestBlock().Header().ID(), 1)
	for it.Next() {
		block := it.Block()
		if block.Header().Number() > endBlockNum {
			break
		}
		receipts, err := chain.GetBlockReceipts(block.Header().ID())
		if err != nil {
			return err
		}
		if err := rlp.Encode(w, &blockAndReceipts{
			block,
			receipts,
		}); err != nil {
			return err
		}
		if progress != nil {
			progress(block.Header().Number())
		}
	}
	return it.Error()
}

func exportState(chain *chain.Chain, kv kv.GetPutter, w io.Writer, endBlockNum uint32) error {
	startBlockNum := endBlockNum - thor.MaxBackTrackingBlockNumber
	if startBlockNum > endBlockNum {
		startBlockNum = 0
	}

	var (
		baseTrie, newTrie *trie.Trie
		err               error
	)
	cIter := chain.NewIterator(chain.BestBlock().Header().ID(), startBlockNum)

	for cIter.Next() {
		block := cIter.Block()
		if block.Header().Number() > endBlockNum {
			break
		}
		baseTrie = newTrie
		newTrie, err = trie.New(block.Header().StateRoot(), kv)
		if err != nil {
			return err
		}
		if err := dumpTrie(baseTrie, newTrie, func(hash thor.Bytes32, leafKey []byte, leafBlob []byte) error {
			if hash != (thor.Bytes32{}) {
				node, err := kv.Get(hash[:])
				if err != nil {
					return err
				}
				fmt.Println(len(node))
				if _, err := w.Write(node); err != nil {
					return err
				}
			}
			// if len(leafBlob) > 0 {
			// 	var acc state.Account
			// 	if err := rlp.DecodeBytes(leafBlob, &acc); err != nil {
			// 		return err
			// 	}
			// 	if len(acc.StorageRoot) > 0 {
			// 		var (
			// 			baseStorageTrie *trie.Trie
			// 			newStorageTrie  *trie.Trie
			// 		)
			// 		if baseTrie != nil {
			// 			v, err := baseTrie.TryGet(leafKey)
			// 			if err != nil {
			// 				return err
			// 			}
			// 			if len(v) > 0 {
			// 				var baseAcc state.Account
			// 				if err := rlp.DecodeBytes(v, &baseAcc); err != nil {
			// 					return err
			// 				}
			// 				if len(baseAcc.StorageRoot) > 0 {
			// 					baseStorageTrie, err = trie.New(thor.BytesToBytes32(baseAcc.StorageRoot), kv)
			// 					if err != nil {
			// 						return err
			// 					}
			// 				}
			// 			}
			// 		}
			// 		newStorageTrie, err = trie.New(thor.BytesToBytes32(acc.StorageRoot), kv)
			// 		if err != nil {
			// 			return err
			// 		}
			// 		if err := dumpTrie(baseStorageTrie, newStorageTrie, func(hash thor.Bytes32, leafKey []byte, leafBlob []byte) error {
			// 			if hash != (thor.Bytes32{}) {
			// 				node, err := kv.Get(hash[:])
			// 				if err != nil {
			// 					return err
			// 				}
			// 				if _, err := w.Write(node); err != nil {
			// 					return err
			// 				}
			// 			}
			// 			return nil
			// 		}); err != nil {
			// 			return err
			// 		}
			// 	}
			// }
			return nil
		}); err != nil {
			return err
		}
	}
	fmt.Println("done")
	return cIter.Error()
}

func dumpTrie(baseTrie, newTrie *trie.Trie, callback func(hash thor.Bytes32, leafKey []byte, leafBlob []byte) error) error {
	var it trie.NodeIterator
	if baseTrie == nil {
		it = newTrie.NodeIterator(nil)
	} else {
		it, _ = trie.NewDifferenceIterator(baseTrie.NodeIterator(nil), newTrie.NodeIterator(nil))
	}

	for it.Next(true) {
		if it.Leaf() {
			if err := callback(it.Hash(), it.LeafKey(), it.LeafBlob()); err != nil {
				return err
			}
		} else {
			if err := callback(it.Hash(), nil, nil); err != nil {
				return err
			}
		}
	}
	return it.Error()
}
