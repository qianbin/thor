// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package muxdb

import (
	"encoding/binary"
)

// trieNodeKeyBuf buffer for trie node key composition.
// A trie node key is composed by [space, ver, path, hash].
// space - 1 byte
// ver - 4 bytes
// name - var len
// path - 8 bytes
// hash - 32 bytes
type trieNodeKeyBuf []byte

func newTrieNodeKeyBuf(name string) trieNodeKeyBuf {
	nameLen := len(name)
	buf := make([]byte, 1+4+nameLen+8+32)
	copy(buf[1+4:], name)
	return buf
}

func (b trieNodeKeyBuf) SetParts(ver uint32, path, hash []byte) {
	binary.BigEndian.PutUint32(b[1:], ver)
	compactPath(b[len(b)-32-8:], path)
	copy(b[len(b)-32:], hash)
}

func (b trieNodeKeyBuf) SetHot() {
	b[0] = trieHotSpace
}

func (b trieNodeKeyBuf) SetCold() {
	b[0] = trieColdSpace
}

func compactPath(dest, path []byte) {
	for i := 0; i < 8; i++ {
		dest[i] = 0
	}

	pathLen := len(path)
	if pathLen > 15 {
		pathLen = 15
	}

	if pathLen > 0 {
		// compact at most 15 nibbles and term with path len.
		for i := 0; i < pathLen; i++ {
			if i%2 == 0 {
				dest[i/2] |= (path[i] << 4)
			} else {
				dest[i/2] |= path[i]
			}
		}
		dest[7] |= byte(pathLen)
	} else {
		// narrow the affected key range of nodes produced by trie commitment.
		dest[0] = (8 << 4)
	}
}
