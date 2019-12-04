// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package chain

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/block"
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/tx"
)

type Branch struct {
	chain     *Chain
	headID    thor.Bytes32
	indexTrie muxdb.Trie
	err       error
}

func newBranch(chain *Chain, headID thor.Bytes32) *Branch {
	_, indexRoot, err := chain.GetBlockHeader(headID)
	if err != nil {
		return &Branch{err: err}
	}

	tr, err := chain.db.NewTrie("i", indexRoot, false)

	return &Branch{
		chain,
		headID,
		tr,
		err,
	}
}

func (b *Branch) HeadID() thor.Bytes32 {
	return b.headID
}

func (b *Branch) GetBlockID(num uint32) (thor.Bytes32, error) {
	if b.err != nil {
		return thor.Bytes32{}, b.err
	}
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], num)
	data, err := b.indexTrie.Get(key[:])
	if err != nil {
		return thor.Bytes32{}, err
	}
	if len(data) == 0 {
		return thor.Bytes32{}, errNotFound
	}
	return thor.BytesToBytes32(data), nil
}

func (b *Branch) GetTransactionMeta(id thor.Bytes32) (*TxMeta, error) {
	if b.err != nil {
		return nil, b.err
	}

	enc, err := b.indexTrie.Get(id[:])
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, errNotFound
	}

	var loc txLocation
	if err := rlp.DecodeBytes(enc, &loc); err != nil {
		return nil, err
	}

	blockID, err := b.GetBlockID(loc.BlockNumber)
	if err != nil {
		return nil, err
	}
	return &TxMeta{blockID, loc.Index, loc.Reverted}, nil
}

func (b *Branch) GetBlockHeader(num uint32) (*block.Header, error) {
	id, err := b.GetBlockID(num)
	if err != nil {
		return nil, err
	}
	h, _, err := b.chain.GetBlockHeader(id)
	return h, err
}

func (b *Branch) GetBlock(num uint32) (*block.Block, error) {
	id, err := b.GetBlockID(num)
	if err != nil {
		return nil, err
	}
	return b.chain.GetBlock(id)
}

func (b *Branch) GetTransaction(id thor.Bytes32) (*tx.Transaction, *TxMeta, error) {
	txMeta, err := b.GetTransactionMeta(id)
	if err != nil {
		return nil, nil, err
	}
	txs, err := b.chain.GetBlockBody(txMeta.BlockID)
	if err != nil {
		return nil, nil, err
	}
	return txs[txMeta.Index], txMeta, nil
}

func (b *Branch) GetReceipt(txID thor.Bytes32) (*tx.Receipt, error) {
	txMeta, err := b.GetTransactionMeta(txID)
	if err != nil {
		return nil, err
	}
	receipts, err := b.chain.GetReceipts(txMeta.BlockID)
	if err != nil {
		return nil, err
	}
	return receipts[txMeta.Index], nil
}

// Exist check if the given block id is on this branch.
func (b *Branch) Exist(id thor.Bytes32) (bool, error) {
	foundID, err := b.GetBlockID(block.Number(id))
	if err != nil {
		if b.chain.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return id == foundID, nil
}

// Diff returns IDs of all blocks which are not on branch specified by otherHeadID.
func (b *Branch) Diff(otherBranch *Branch) ([]thor.Bytes32, error) {
	var ids []thor.Bytes32
	for i := int64(block.Number(b.headID)); i >= 0; i-- {
		id, err := b.GetBlockID(uint32(i))
		if err != nil {
			return nil, err
		}
		exist, err := b.Exist(id)
		if err != nil {
			return nil, err
		}
		if exist {
			break
		}
		ids = append(ids, id)
	}
	// reverse
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
	}
	return ids, nil
}

type txLocation struct {
	BlockNumber uint32
	Index       uint64
	Reverted    bool
}

func (c *Chain) indexBlock(indexRoot thor.Bytes32, block *block.Block, receipts tx.Receipts) (thor.Bytes32, error) {
	trie, err := c.db.NewTrie("i", indexRoot, false)
	if err != nil {
		return thor.Bytes32{}, err
	}
	id := block.Header().ID()

	// map block number to block ID
	if err := trie.Update(id[:4], id[:]); err != nil {
		return thor.Bytes32{}, err
	}

	// record tx locations
	num := block.Header().Number()
	for i, tx := range block.Transactions() {
		enc, err := rlp.EncodeToBytes(&txLocation{
			BlockNumber: num,
			Index:       uint64(i),
			Reverted:    receipts[i].Reverted,
		})
		if err != nil {
			return thor.Bytes32{}, err
		}
		if err := trie.Update(tx.ID().Bytes(), enc); err != nil {
			return thor.Bytes32{}, err
		}
	}
	return trie.Commit()
}
