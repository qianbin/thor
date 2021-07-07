// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"hash"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/thor"
)

type hasher struct {
	tmp sliceBuffer
	sha hash.Hash
}

type sliceBuffer []byte

func (b *sliceBuffer) Write(data []byte) (n int, err error) {
	*b = append(*b, data...)
	return len(data), nil
}

func (b *sliceBuffer) Reset() {
	*b = (*b)[:0]
}

// hashers live in a global pool.
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{
			tmp: make(sliceBuffer, 0, 550), // cap is as large as a full fullNode.
			sha: thor.NewBlake2b(),
		}
	},
}

func newHasher() *hasher {
	h := hasherPool.Get().(*hasher)
	return h
}

func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hash(n node, db DatabaseWriter, path []byte, force bool, newVer uint32) (node, node, [][]byte, error) {
	// If we're not storing the node, just hashing, use available cached data
	if hash, dirty := n.cache(); hash != nil {
		if db == nil {
			return hash, n, nil, nil
		}
		if !dirty && !force { // !force is non-root node, that means root node is always stored
			switch n.(type) {
			case *fullNode, *shortNode:
				return hash, hash, nil, nil
			default:
				return hash, n, nil, nil
			}
		}
	}
	// Trie not processed yet or needs storage, walk the children
	collapsed, cached, cVers, metaList, err := h.hashChildren(n, db, path, newVer)
	if err != nil {
		return nil, n, nil, err
	}
	hashed, err := h.store(collapsed, db, path, force, newVer, cVers, metaList)
	if err != nil {
		return nil, n, nil, err
	}
	if _, ok := hashed.(*hashNode); ok {
		// stored
		metaList = nil
	}
	// Cache the hash of the node for later reuse and remove
	// the dirty flag in commit mode. It's fine to assign these values directly
	// without copying the node first because hashChildren copies it.
	cachedHash, _ := hashed.(*hashNode)
	switch cn := cached.(type) {
	case *shortNode:
		cn.flags.hash = cachedHash
		if db != nil {
			cn.flags.dirty = false
		}
	case *fullNode:
		cn.flags.hash = cachedHash
		if db != nil {
			cn.flags.dirty = false
		}
	}
	return hashed, cached, metaList, nil
}

// hashChildren replaces the children of a node with their hashes if the encoded
// size of the child is larger than a hash, returning the collapsed node as well
// as a replacement for the original node with the child hashes cached in.
func (h *hasher) hashChildren(original node, db DatabaseWriter, path []byte, newVer uint32) (node, node, []uint32, [][]byte, error) {
	var (
		err      error
		metaList [][]byte
	)

	switch n := original.(type) {
	case *shortNode:
		// Hash the short node's child, caching the newly hashed subtree
		collapsed, cached := n.copy(), n.copy()
		collapsed.Key = hexToCompact(n.Key)
		cached.Key = common.CopyBytes(n.Key)
		var cVer uint32
		if vn, ok := n.Val.(*valueNode); !ok {
			var ml [][]byte
			collapsed.Val, cached.Val, ml, err = h.hash(n.Val, db, append(path, n.Key...), false, newVer)
			if err != nil {
				return original, original, nil, nil, err
			}
			if v := collapsed.Val.version(); v != 0 {
				cVer = v
			}
			metaList = append(metaList, ml...)
		} else {
			metaList = append(metaList, vn.meta)
		}
		if collapsed.Val == nil {
			collapsed.Val = &valueNode{} // Ensure that nil children are encoded as empty strings.
		}
		return collapsed, cached, []uint32{cVer}, metaList, nil
	case *fullNode:
		// Hash the full node's children, caching the newly hashed subtrees
		collapsed, cached := n.copy(), n.copy()
		var cVers [16]uint32
		for i := 0; i < 16; i++ {
			if n.Children[i] != nil {
				var ml [][]byte
				collapsed.Children[i], cached.Children[i], ml, err = h.hash(n.Children[i], db, append(path, byte(i)), false, newVer)
				if err != nil {
					return original, original, nil, nil, err
				}
				cVers[i] = collapsed.Children[i].version()
				metaList = append(metaList, ml...)
			} else {
				collapsed.Children[i] = &valueNode{} // Ensure that nil children are encoded as empty strings.
			}
		}
		if collapsed.Children[16] == nil {
			collapsed.Children[16] = &valueNode{}
		} else {
			if vn, ok := collapsed.Children[16].(*valueNode); ok {
				metaList = append(metaList, vn.meta)
			}
		}
		return collapsed, cached, cVers[:], metaList, nil

	default:
		// Value and hash nodes don't have children so they're left as were
		return n, original, nil, nil, nil
	}
}

func (h *hasher) store(n node, db DatabaseWriter, path []byte, force bool, newVer uint32, cVers []uint32, metaList [][]byte) (node, error) {
	// Don't store hashes or empty nodes.
	if _, isHash := n.(*hashNode); n == nil || isHash {
		return n, nil
	}
	// Generate the RLP encoding of the node
	h.tmp.Reset()
	if err := rlp.Encode(&h.tmp, n); err != nil {
		panic("encode error: " + err.Error())
	}

	if len(h.tmp) < 32 && !force {
		return n, nil // Nodes smaller than 32 bytes are stored inside their parent
	}
	// Larger nodes are replaced by their hash and stored in the database.
	hash, _ := n.cache()
	if hash == nil {
		h.sha.Reset()
		h.sha.Write(h.tmp)
		hash = &hashNode{hash: h.sha.Sum(nil)}
	}
	if db != nil {
		if ex, ok := db.(DatabaseWriterEx); ok {
			if len(cVers) == 1 {
				if err := rlp.Encode(&h.tmp, cVers[0]); err != nil {
					return nil, err
				}
			} else {
				if err := rlp.Encode(&h.tmp, cVers); err != nil {
					return nil, err
				}
			}

			if err := rlp.Encode(&h.tmp, metaList); err != nil {
				return nil, err
			}

			hash.ver = newVer
			return hash, ex.PutEx(&CompositKey{
				hash.hash,
				newVer,
				path,
				false,
			}, CompositValue(h.tmp))
		}
		return hash, db.Put(hash.hash, h.tmp)
	}
	return hash, nil
}
