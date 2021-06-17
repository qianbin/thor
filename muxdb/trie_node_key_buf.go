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
// name - var len
// path - 8 bytes
// ver - 4 bytes
// hash - 32 bytes
type trieNodeKeyBuf []byte

func newTrieNodeKeyBuf(name string) trieNodeKeyBuf {
	nameLen := len(name)
	buf := make([]byte, 1+nameLen+8+4+32)
	copy(buf[1:], name)
	return buf
}

func (b trieNodeKeyBuf) SetParts(ver uint32, path, hash []byte) {
	binary.BigEndian.PutUint32(b[len(b)-32-4:], ver)
	compactPath(b[len(b)-32-4-8:], path)
	copy(b[len(b)-32:], hash)
	if len(path) > 5 {
		b[0] = trieColdSpace
	} else {
		b[0] = trieHotSpace
	}
}

func compactPath(dest, path []byte) {
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
	binary.BigEndian.PutUint64(dest, v)
}

// func compactPath(dest, path []byte) {
// 	pathLen := len(path)
// 	if pathLen > 15 {
// 		pathLen = 15
// 	}
// 	dest[0] = byte(pathLen) << 4
// 	for i := 0; i < 15; i++ {
// 		x := byte(0)
// 		if i < len(path) {
// 			x = path[i]
// 		}
// 		if i%2 == 0 {
// 			dest[(i+1)/2] |= x
// 		} else {
// 			dest[(i+1)/2] = (x << 4)
// 		}
// 	}
// }
