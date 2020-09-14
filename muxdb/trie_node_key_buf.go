// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"encoding/binary"

	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/trie"
)

// trieNodeKeyBuf buffer for trie node key composition.
// A trie node key is composed by [space, name, path, hash].
// space - 1 byte
// name - var len
// path - 8 bytes
// hash - 32 bytes
type trieNodeKeyBuf []byte

func newTrieNodeKeyBuf(name string) trieNodeKeyBuf {
	nameLen := len(name)
	buf := make([]byte, 1+nameLen+4+32)
	copy(buf[1:], name)
	return buf
}

func (b trieNodeKeyBuf) spaceSlot() *byte {
	return &b[0]
}

func (b trieNodeKeyBuf) revSlot() []byte {
	offset := len(b) - 32 - 4
	return b[offset : offset+4]
}

func (b trieNodeKeyBuf) hashSlot() []byte {
	offset := len(b) - 32
	return b[offset:]
}

// Get gets encoded trie node from kv store.
func (b trieNodeKeyBuf) Get(get kv.GetFunc, key *trie.NodeKey) ([]byte, error) {
	binary.BigEndian.PutUint32(b.revSlot(), key.Revision)
	copy(b.hashSlot(), key.Hash)

	spaceSlot := b.spaceSlot()

	// try to get from permanat space
	*spaceSlot = trieSpaceP
	if val, err := get(b); err == nil {
		return val, nil
	}

	// then live space a
	*spaceSlot = trieSpaceA
	if val, err := get(b); err == nil {
		return val, nil
	}

	// finally space b
	*spaceSlot = trieSpaceB
	return get(b)
}

// Put put encoded trie node to the given space.
func (b trieNodeKeyBuf) Put(put kv.PutFunc, key *trie.NodeKey, enc []byte, space byte) error {
	*b.spaceSlot() = space
	binary.BigEndian.PutUint32(b.revSlot(), key.Revision)
	copy(b.hashSlot(), key.Hash)

	return put(b, enc)
}
