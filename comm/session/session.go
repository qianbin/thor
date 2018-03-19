package session

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/vechain/thor/p2psrv"
	"github.com/vechain/thor/thor"
)

const (
	maxKnownTxs    = 32768 // Maximum transactions hashes to keep in the known list (prevent DOS)
	maxKnownBlocks = 1024  // Maximum block hashes to keep in the known list (prevent DOS)
)

// Session abstraction of a p2p connection.
type Session struct {
	peer        *p2psrv.Peer
	knownTxs    *lru.Cache
	knownBlocks *lru.Cache
	trunkHead   struct {
		sync.Mutex
		bestBlockID thor.Hash
		totalScore  uint64
	}
}

// New create a Session instance.
func New(peer *p2psrv.Peer) *Session {
	knownTxs, _ := lru.New(maxKnownTxs)
	knownBlocks, _ := lru.New(maxKnownBlocks)
	return &Session{
		peer:        peer,
		knownTxs:    knownTxs,
		knownBlocks: knownBlocks,
	}
}

// Peer returns the connection to remote peer.
func (s *Session) Peer() *p2psrv.Peer {
	return s.peer
}

// TrunkHead returns trunk head of the remote chain.
func (s *Session) TrunkHead() (bestBlockID thor.Hash, totalScore uint64) {
	s.trunkHead.Lock()
	defer s.trunkHead.Unlock()
	return s.trunkHead.bestBlockID, s.trunkHead.totalScore
}

// UpdateTrunkHead udpate trunk head of of the remote chain.
func (s *Session) UpdateTrunkHead(bestBlockID thor.Hash, totalScore uint64) {
	s.trunkHead.Lock()
	defer s.trunkHead.Unlock()
	s.trunkHead.bestBlockID, s.trunkHead.totalScore = bestBlockID, totalScore
}

// MarkTransaction marks a transaction to known.
func (s *Session) MarkTransaction(id thor.Hash) {
	s.knownTxs.Add(id, struct{}{})
}

// MarkBlock marks a block to known.
func (s *Session) MarkBlock(id thor.Hash) {
	s.knownBlocks.Add(id, struct{}{})
}

// IsTransactionKnown returns if the transaction is known.
func (s *Session) IsTransactionKnown(id thor.Hash) bool {
	return s.knownTxs.Contains(id)
}

// IsBlockKnown returns if the block is known.
func (s *Session) IsBlockKnown(id thor.Hash) bool {
	return s.knownBlocks.Contains(id)
}
