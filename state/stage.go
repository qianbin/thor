// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package state

import (
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/thor"
)

// Stage abstracts changes on the main accounts trie.
type Stage struct {
	db           *muxdb.MuxDB
	accountTrie  *muxdb.Trie
	storageTries []*muxdb.Trie
	codes        map[thor.Bytes32][]byte
	ver          uint32
}

// Hash computes hash of the main accounts trie.
func (s *Stage) Hash() thor.Bytes32 {
	return s.accountTrie.Hash()
}

// Commit commits all changes into main accounts trie and storage tries.
func (s *Stage) Commit(handleTries func(*muxdb.Trie) error) (thor.Bytes32, error) {
	codeStore := s.db.NewStore(codeStoreName)

	// write codes
	if err := codeStore.Batch(func(w kv.PutFlusher) error {
		for hash, code := range s.codes {
			if err := w.Put(hash[:], code); err != nil {
				return &Error{err}
			}
		}
		return nil
	}); err != nil {
		return thor.Bytes32{}, &Error{err}
	}

	// commit storage tries
	for _, t := range s.storageTries {
		if handleTries != nil {
			if err := handleTries(t); err != nil {
				return thor.Bytes32{}, err
			}
		}
		if _, err := t.CommitVer(s.ver + 1); err != nil {
			return thor.Bytes32{}, &Error{err}
		}
	}
	if handleTries != nil {
		if err := handleTries(s.accountTrie); err != nil {
			return thor.Bytes32{}, err
		}
	}
	// commit accounts trie
	return s.accountTrie.Commit()
}
