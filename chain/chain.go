// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package chain

import (
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/vechain/thor/block"
	"github.com/vechain/thor/co"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/triex"
	"github.com/vechain/thor/tx"
)

var (
	errNotFound  = errors.New("not found")
	bestBlockKey = []byte("best")
)

// Chain describes a persistent block chain.
// It's thread-safe.
type Chain struct {
	kv            kv.GetPutter
	triex         *triex.Proxy
	genesisBlock  *block.Block
	bestBlock     *block.Block
	tag           byte
	headerCache   *cache
	bodyCache     *cache
	receiptsCache *cache
	tick          co.Signal
	rw            sync.RWMutex
}

// New create an instance of Chain.
func New(kv kv.GetPutter, triex *triex.Proxy, genesisBlock *block.Block) (*Chain, error) {
	if genesisBlock.Header().Number() != 0 {
		return nil, errors.New("genesis number != 0")
	}
	if len(genesisBlock.Transactions()) != 0 {
		return nil, errors.New("genesis block should not have transactions")
	}
	genesisID := genesisBlock.Header().ID()

	chain := &Chain{
		kv:            kv,
		triex:         triex,
		genesisBlock:  genesisBlock,
		tag:           genesisID[31],
		headerCache:   newCache(8192),
		bodyCache:     newCache(256),
		receiptsCache: newCache(256),
	}

	if bestBlockID, err := chain.loadBestBlockID(); err != nil {
		if !kv.IsNotFound(err) {
			return nil, err
		}

		indexRoot, err := indexBlock(triex, thor.Bytes32{}, genesisBlock, nil)
		if err != nil {
			return nil, err
		}

		if err := chain.saveBlockAndReceipts(genesisBlock, nil, indexRoot); err != nil {
			return nil, err
		}
		chain.bestBlock = genesisBlock
	} else {
		existGenesisID, err := chain.NewBranch(bestBlockID).GetBlockID(0)
		if err != nil {
			return nil, err
		}

		if existGenesisID != genesisID {
			return nil, errors.New("genesis mismatch")
		}

		b, err := chain.GetBlock(bestBlockID)
		if err != nil {
			return nil, err
		}

		chain.bestBlock = b
	}

	return chain, nil
}

// Tag returns chain tag, which is the last byte of genesis id.
func (c *Chain) Tag() byte {
	return c.tag
}

// GenesisBlock returns genesis block.
func (c *Chain) GenesisBlock() *block.Block {
	return c.genesisBlock
}

// BestBlock returns the newest block on trunk.
func (c *Chain) BestBlock() *block.Block {
	c.rw.RLock()
	defer c.rw.RUnlock()
	return c.bestBlock
}

// SetBestBlock allow reseting best block.
func (c *Chain) SetBestBlock(id thor.Bytes32) error {
	b, err := c.GetBlock(id)
	if err != nil {
		return err
	}
	return c.setBestBlock(b)
}

func (c *Chain) setBestBlock(b *block.Block) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	if err := c.saveBestBlockID(b.Header().ID()); err != nil {
		return err
	}

	c.bestBlock = b
	return nil
}

// AddBlock add a new block into block chain.
// If the new block already exist, it quickly returns without error.
func (c *Chain) AddBlock(newBlock *block.Block, receipts tx.Receipts) error {
	newBlockID := newBlock.Header().ID()

	if _, _, err := c.GetBlockHeader(newBlockID); err != nil {
		if !c.IsNotFound(err) {
			return err
		}
	} else {
		// already exist
		return nil
	}

	parentID := newBlock.Header().ParentID()

	_, parentIndexRoot, err := c.GetBlockHeader(parentID)
	if err != nil {
		if c.IsNotFound(err) {
			return errors.New("parent missing")
		}
		return err
	}

	indexRoot, err := indexBlock(c.triex, parentIndexRoot, newBlock, receipts)
	if err != nil {
		return err
	}

	if err := c.saveBlockAndReceipts(newBlock, receipts, indexRoot); err != nil {
		return err
	}

	if newBlock.Header().BetterThan(c.BestBlock().Header()) {
		if err := c.setBestBlock(newBlock); err != nil {
			return err
		}
	}

	c.tick.Broadcast()
	return nil
}

// GetBlockHeader get block header and index root by block id.
func (c *Chain) GetBlockHeader(id thor.Bytes32) (*block.Header, thor.Bytes32, error) {
	val, err := c.headerCache.GetOrLoad(id, func(interface{}) (interface{}, error) {
		return loadBlockHeader(c.kv, id)
	})
	if err != nil {
		return nil, thor.Bytes32{}, err
	}
	extHeader := val.(*extHeader)
	return extHeader.Header, extHeader.IndexRoot, nil
}

// GetBlock get block by id.
func (c *Chain) GetBlock(id thor.Bytes32) (*block.Block, error) {
	header, _, err := c.GetBlockHeader(id)
	if err != nil {
		return nil, err
	}

	txsRoot := header.TxsRoot()
	if txsRoot == emptyRoot {
		return block.Compose(header, nil), nil
	}

	txs, err := c.bodyCache.GetOrLoad(txsRoot, func(interface{}) (interface{}, error) {
		return loadTransactions(c.kv, txsRoot)
	})
	if err != nil {
		return nil, err
	}
	return block.Compose(header, txs.(tx.Transactions)), nil
}

// GetReceipts get all receipts in the block of given block id.
func (c *Chain) GetReceipts(id thor.Bytes32) (tx.Receipts, error) {
	header, _, err := c.GetBlockHeader(id)
	if err != nil {
		return nil, err
	}
	receiptsRoot := header.ReceiptsRoot()
	if receiptsRoot == emptyRoot {
		return nil, nil
	}

	receipts, err := c.receiptsCache.GetOrLoad(receiptsRoot, func(interface{}) (interface{}, error) {
		return loadReceipts(c.kv, receiptsRoot)
	})
	if err != nil {
		return nil, err
	}
	return receipts.(tx.Receipts), nil
}

// NewSeeker returns a new seeker instance.
func (c *Chain) NewSeeker(headID thor.Bytes32) *Seeker {
	return newSeeker(c, c.NewBranch(headID))
}

// IsNotFound returns if an error means not found.
func (c *Chain) IsNotFound(err error) bool {
	return err == errNotFound || c.kv.IsNotFound(err)
}

// NewTicker create a signal Waiter to receive event of head block change.
func (c *Chain) NewTicker() co.Waiter {
	return c.tick.NewWaiter()
}

// NewTrunk create a branch with best block as head.
func (c *Chain) NewTrunk() *Branch {
	return newBranch(c, c.BestBlock().Header().ID())
}

// NewBranch create a branch with head block specified by headID.
func (c *Chain) NewBranch(headID thor.Bytes32) *Branch {
	return newBranch(c, headID)
}

// loadBestBlockID returns the best block ID on trunk.
func (c *Chain) loadBestBlockID() (thor.Bytes32, error) {
	data, err := c.triex.GetArbitrary(bestBlockKey)
	if err != nil {
		return thor.Bytes32{}, err
	}
	return thor.BytesToBytes32(data), nil
}

// saveBestBlockID save the best block ID on trunk.
func (c *Chain) saveBestBlockID(id thor.Bytes32) error {
	return c.triex.PutArbitrary(bestBlockKey, id[:])
}
func (c *Chain) saveBlockAndReceipts(
	block *block.Block,
	receipts tx.Receipts,
	indexRoot thor.Bytes32,
) error {
	var (
		header       = block.Header()
		txs          = block.Transactions()
		txsRoot      = header.TxsRoot()
		receiptsRoot = header.ReceiptsRoot()
		extHeader    = &extHeader{header, indexRoot}
		batch        = c.kv.NewBatch()
	)

	if txsRoot != emptyRoot {
		if err := saveTransactions(batch, txsRoot, txs); err != nil {
			return err
		}
	}
	if receiptsRoot != emptyRoot {
		if err := saveReceipts(batch, receiptsRoot, receipts); err != nil {
			return err
		}
	}

	if err := saveBlockHeader(batch, extHeader); err != nil {
		return err
	}

	if err := batch.Write(); err != nil {
		return err
	}

	if receiptsRoot != emptyRoot {
		c.receiptsCache.Add(receiptsRoot, receipts)
	}

	if txsRoot != emptyRoot {
		c.bodyCache.Add(txsRoot, txs)
	}

	c.headerCache.Add(header.ID(), extHeader)

	return nil
}

// Block expanded block.Block to indicate whether it is obsolete
type Block struct {
	*block.Block
	Obsolete bool
}

// BlockReader defines the interface to read Block
type BlockReader interface {
	Read() ([]*Block, error)
}

type readBlock func() ([]*Block, error)

func (r readBlock) Read() ([]*Block, error) {
	return r()
}

// NewBlockReader generate an object that implements the BlockReader interface
func (c *Chain) NewBlockReader(position thor.Bytes32) BlockReader {
	return readBlock(func() ([]*Block, error) {

		bestID := c.BestBlock().Header().ID()
		if bestID == position {
			return nil, nil
		}
		branch := c.NewBranch(bestID)

		var blocks []*Block
		for {
			positionBlock, err := c.GetBlock(position)
			if err != nil {
				return nil, err
			}

			if block.Number(position) > block.Number(bestID) {
				blocks = append(blocks, &Block{positionBlock, true})
				position = positionBlock.Header().ParentID()
				continue
			}

			onBranch, err := branch.Exist(position)
			if err != nil {
				return nil, err
			}

			if onBranch {
				next, err := branch.GetBlock(block.Number(position) + 1)
				if err != nil {
					return nil, err
				}

				position = next.Header().ID()
				return append(blocks, &Block{next, false}), nil
			}

			blocks = append(blocks, &Block{positionBlock, true})
			position = positionBlock.Header().ParentID()
		}
	})
}

// NewIterator create a block iterator, to fast iterate blocks from lower number to higher.
// It's much faster than get block one by one.
func (c *Chain) NewIterator(bufSize int) *Iterator {

	return &Iterator{
		chain:   c,
		branch:  c.NewTrunk(),
		bufSize: bufSize,
	}
}

// Iterator block iterator.
type Iterator struct {
	chain   *Chain
	branch  *Branch
	bufSize int

	nextNum uint32
	buf     []*block.Block
	err     error
}

// Error returns occurred error.
func (i *Iterator) Error() error {
	return i.err
}

// Seek seek to given block number as start position.
// Error will be reset.
func (i *Iterator) Seek(num uint32) *Iterator {
	i.nextNum = num
	i.buf = nil
	i.err = nil
	return i
}

// Next move the iterator to next.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	if bufLen := len(i.buf); bufLen > 1 {
		// pop last one
		i.buf = i.buf[:bufLen-1]
		return true
	}

	i.buf = nil

	headNum := block.Number(i.branch.HeadID())

	if i.nextNum > headNum {
		return false
	}

	toNum := i.nextNum + uint32(i.bufSize)
	if toNum > headNum {
		toNum = headNum
	}

	id, err := i.branch.GetBlockID(toNum)
	if err != nil {
		i.err = err
		return false
	}

	var buf []*block.Block
	for block.Number(id) >= i.nextNum && block.Number(id) != math.MaxUint32 {
		b, err := i.chain.GetBlock(id)
		if err != nil {
			i.err = err
			return false
		}
		buf = append(buf, b)
		// traverse by parent id to save read io
		id = b.Header().ParentID()
	}

	i.buf = buf
	i.nextNum = toNum + 1
	return true
}

// Block returns current block.
func (i *Iterator) Block() *block.Block {
	if bufLen := len(i.buf); bufLen > 0 {
		return i.buf[bufLen-1]
	}
	return nil
}
