// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package state

import (
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/muxdb/kv"
	"github.com/vechain/thor/thor"
)

// Stage abstracts changes on the main accounts trie.
type Stage struct {
	err error

	accountTrie  muxdb.Trie
	storageTries []muxdb.Trie
	codes        []codeWithHash
	codeStore    kv.Store
}

type codeWithHash struct {
	code []byte
	hash []byte
}

func newStage(db *muxdb.MuxDB, root thor.Bytes32, changes map[thor.Address]*changedObject) *Stage {

	accountTrie, err := db.NewTrie("a", root, true)
	if err != nil {
		return &Stage{err: err}
	}

	storageTries := make([]muxdb.Trie, 0, len(changes))
	codes := make([]codeWithHash, 0, len(changes))

	for addr, obj := range changes {
		dataCpy := obj.data

		if len(obj.code) > 0 {
			codes = append(codes, codeWithHash{
				code: obj.code,
				hash: dataCpy.CodeHash})
		}

		// skip storage changes if account is empty
		if !dataCpy.IsEmpty() {
			if len(obj.storage) > 0 {
				strie, err := db.NewTrie("s"+string(thor.Blake2b(addr[:]).Bytes()), thor.BytesToBytes32(dataCpy.StorageRoot), true)
				if err != nil {
					return &Stage{err: err}
				}
				storageTries = append(storageTries, strie)
				for k, v := range obj.storage {
					if err := strie.Update(k[:], v); err != nil {
						return &Stage{err: err}
					}
				}

				dataCpy.StorageRoot = strie.Hash().Bytes()
			}
		}

		if err := saveAccount(accountTrie, addr, &dataCpy); err != nil {
			return &Stage{err: err}
		}
	}
	return &Stage{
		accountTrie:  accountTrie,
		storageTries: storageTries,
		codes:        codes,
		codeStore:    db.NewStore("c/"),
	}
}

// Hash computes hash of the main accounts trie.
func (s *Stage) Hash() (thor.Bytes32, error) {
	if s.err != nil {
		return thor.Bytes32{}, s.err
	}
	return s.accountTrie.Hash(), nil
}

// Commit commits all changes into main accounts trie and storage tries.
func (s *Stage) Commit() (thor.Bytes32, error) {
	if s.err != nil {
		return thor.Bytes32{}, s.err
	}

	// write codes
	if err := s.codeStore.Batch(func(w kv.PutCommitter) error {
		for _, code := range s.codes {
			if err := w.Put(code.hash, code.code); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return thor.Bytes32{}, err
	}

	// commit storage tries
	for _, strie := range s.storageTries {
		_, err := strie.Commit()
		if err != nil {
			return thor.Bytes32{}, err
		}
	}

	// commit accounts trie
	root, err := s.accountTrie.Commit()
	if err != nil {
		return thor.Bytes32{}, err
	}

	return root, nil
}
