// Copyright 2014 The go-ethereum Authors
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

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/inconshreveable/log15"
	"github.com/vechain/thor/thor"
)

var log = log15.New("pkg", "trie")

var (
	// This is the known root hash of an empty trie.
	emptyRoot = thor.Blake2b(rlp.EmptyString)
	// This is the known hash of an empty state trie entry.
	emptyState = thor.Blake2b(nil)
)

// Database must be implemented by backing stores for the trie.
type Database interface {
	DatabaseReader
	DatabaseWriter
}

// DatabaseReader wraps the Get method of a backing store for the trie.
type DatabaseReader interface {
	Get(key []byte) (value []byte, err error)
	Has(key []byte) (bool, error)
}

// DatabaseWriter wraps the Put method of a backing store for the trie.
type DatabaseWriter interface {
	// Put stores the mapping key->value in the database.
	// Implementations must not hold onto the value bytes, the trie
	// will reuse the slice across calls to Put.
	Put(key, value []byte) error
}

// CompositKey contains extra info beside the node hash.
type CompositKey struct {
	Hash    []byte
	Ver     uint32 // the version number
	Path    []byte // the radix path of key
	Scaning bool   // whether the key is being iterated. might be useful for cache logic.
}

// CompositValue consists of three parts: < node | child versions | extra >
type CompositValue []byte

// Split splits the value into parts.
func (v CompositValue) Split() ([]byte, []byte, []byte, error) {
	_, _, rest, err := rlp.Split(v)
	if err != nil {
		return nil, nil, nil, err
	}
	node := v[:len(v)-len(rest)]

	_, _, metaList, err := rlp.Split(rest)
	if err != nil {
		return nil, nil, nil, err
	}
	vers := rest[:len(rest)-len(metaList)]
	return node, vers, metaList, nil
}

// DatabaseReaderEx extended reader.
type DatabaseReaderEx interface {
	GetEx(key *CompositKey) (CompositValue, error)
}

// DatabaseWriterEx extended writer.
type DatabaseWriterEx interface {
	PutEx(key *CompositKey, val CompositValue) error
}

type DirectReader interface {
	DirectGet(key []byte, nodeVer uint32) ([]byte, []byte, bool, error)
}

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
type Trie struct {
	root node
	db   Database
}

// newFlag returns the cache flag value for a newly created node.
func (t *Trie) newFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the blake2b hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root thor.Bytes32, db Database) (*Trie, error) {
	return NewVersioned(root, db, 0)
}

// New creates a trie with an existing root node and its version number from db.
// It requires the db implements DatabaseReaderEx interface.
func NewVersioned(root thor.Bytes32, db Database, ver uint32) (*Trie, error) {
	trie := &Trie{db: db}
	if (root != thor.Bytes32{}) && root != emptyRoot {
		if db == nil {
			panic("trie.New: cannot use existing root without a database")
		}
		rootnode, _, err := trie.resolveHash(&hashNode{root[:], ver}, nil, false)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}
	return trie, nil
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIterator(start []byte) NodeIterator {
	return newNodeIterator(t, start, nil)
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIteratorWithFilter(start []byte, filter func(path []byte, ver uint32) bool) NodeIterator {
	return newNodeIterator(t, start, filter)
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *Trie) Get(key []byte) []byte {
	res, err := t.TryGet(key)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryGet(key []byte) ([]byte, error) {
	rawKey := key
	key = keybytesToHex(key)
	value, _, newroot, didResolve, err := t.tryGet(t.root, rawKey, key, 0)
	if err == nil && didResolve {
		t.root = newroot
	}
	return value, err
}

func (t *Trie) TryGetMeta(key []byte) ([]byte, []byte, error) {
	rawKey := key
	key = keybytesToHex(key)
	value, meta, newroot, didResolve, err := t.tryGet(t.root, rawKey, key, 0)
	if err == nil && didResolve {
		t.root = newroot
	}

	return value, meta, err
}

var n int64 = 0
var totalPathLen int64 = 0
var miss int64 = 0

func init() {
	go func() {
		for {
			<-time.After(time.Second * 30)
			count := atomic.LoadInt64(&n)
			totalLen := atomic.LoadInt64(&totalPathLen)
			log.Info(fmt.Sprintf("%v %v %v", count, miss, totalLen))
		}
	}()
}
func add(plen int64) {
	atomic.AddInt64(&n, 1)
	atomic.AddInt64(&totalPathLen, plen)
}
func setMiss() {
	atomic.AddInt64(&n, 1)
	atomic.AddInt64(&miss, 1)

}

func (t *Trie) tryGet(origNode node, rawKey, key []byte, pos int) (value, meta []byte, newnode node, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, nil, false, nil
	case *valueNode:
		return n.value, n.meta, n, false, nil
	case *shortNode:
		if len(key)-pos < len(n.Key) || !bytes.Equal(n.Key, key[pos:pos+len(n.Key)]) {
			// key not found in trie
			return nil, nil, n, false, nil
		}
		value, meta, newnode, didResolve, err = t.tryGet(n.Val, rawKey, key, pos+len(n.Key))
		if err == nil && didResolve {
			n = n.copy()
			n.Val = newnode
		}
		return value, meta, n, didResolve, err
	case *fullNode:
		value, meta, newnode, didResolve, err = t.tryGet(n.Children[key[pos]], rawKey, key, pos+1)
		if err == nil && didResolve {
			n = n.copy()
			n.Children[key[pos]] = newnode
		}
		return value, meta, n, didResolve, err
	case *hashNode:
		if dr, ok := t.db.(DirectReader); ok {
			var found bool
			if value, meta, found, err = dr.DirectGet(rawKey, n.ver); err != nil {
				return nil, nil, nil, false, err
			}

			if found {
				add(int64(pos))
				return value, meta, nil, false, nil
			} else {
				setMiss()
			}
		}
		child, _, err := t.resolveHash(n, key[:pos], false)
		if err != nil {
			return nil, nil, n, true, err
		}
		value, meta, newnode, _, err := t.tryGet(child, rawKey, key, pos)
		return value, meta, newnode, true, err
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *Trie) Update(key, value []byte) {
	if err := t.TryUpdate(key, value); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryUpdate associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryUpdate(key, value []byte) error {
	return t.TryUpdateMeta(key, value, nil)
}

func (t *Trie) TryUpdateMeta(key, value []byte, meta []byte) error {
	k := keybytesToHex(key)
	if len(value) != 0 {
		_, n, err := t.insert(t.root, nil, k, &valueNode{value: value, meta: meta})
		if err != nil {
			return err
		}
		t.root = n
	} else {
		_, n, err := t.delete(t.root, nil, k)
		if err != nil {
			return err
		}
		t.root = n
	}
	return nil
}

func (t *Trie) insert(n node, prefix, key []byte, value node) (bool, node, error) {
	if len(key) == 0 {
		if v, ok := n.(*valueNode); ok {
			return !bytes.Equal(v.value, value.(*valueNode).value), value, nil
		}
		return true, value, nil
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(n.Key) {
			dirty, nn, err := t.insert(n.Val, append(prefix, key[:matchlen]...), key[matchlen:], value)
			if !dirty || err != nil {
				return false, n, err
			}
			flag := t.newFlag()
			return true, &shortNode{n.Key, nn, flag}, nil
		}
		// Otherwise branch out at the index where they differ.
		branch := &fullNode{flags: t.newFlag()}
		var err error
		_, branch.Children[n.Key[matchlen]], err = t.insert(nil, append(prefix, n.Key[:matchlen+1]...), n.Key[matchlen+1:], n.Val)
		if err != nil {
			return false, nil, err
		}
		_, branch.Children[key[matchlen]], err = t.insert(nil, append(prefix, key[:matchlen+1]...), key[matchlen+1:], value)

		if err != nil {
			return false, nil, err
		}
		// Replace this shortNode with the branch if it occurs at index 0.
		if matchlen == 0 {
			return true, branch, nil
		}
		// Otherwise, replace it with a short node leading up to the branch.
		return true, &shortNode{key[:matchlen], branch, t.newFlag()}, nil
	case *fullNode:
		dirty, nn, err := t.insert(n.Children[key[0]], append(prefix, key[0]), key[1:], value)
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn
		return true, n, nil
	case nil:
		flag := t.newFlag()
		return true, &shortNode{key, value, flag}, nil

	case *hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, _, err := t.resolveHash(n, prefix, false)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.insert(rn, prefix, key, value)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *Trie) Delete(key []byte) {
	if err := t.TryDelete(key); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryDelete(key []byte) error {
	k := keybytesToHex(key)
	_, n, err := t.delete(t.root, nil, k)
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(n node, prefix, key []byte) (bool, node, error) {
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		if matchlen < len(n.Key) {
			return false, n, nil // don't replace n on mismatch
		}
		if matchlen == len(key) {
			return true, nil, nil // remove n entirely for whole matches
		}
		// The key is longer than n.Key. Remove the remaining suffix
		// from the subtrie. Child can never be nil here since the
		// subtrie must contain at least two other values with keys
		// longer than n.Key.
		dirty, child, err := t.delete(n.Val, append(prefix, key[:len(n.Key)]...), key[len(n.Key):])
		if !dirty || err != nil {
			return false, n, err
		}
		switch child := child.(type) {
		case *shortNode:
			// Deleting from the subtrie reduced it to another
			// short node. Merge the nodes to avoid creating a
			// shortNode{..., shortNode{...}}. Use concat (which
			// always creates a new slice) instead of append to
			// avoid modifying n.Key since it might be shared with
			// other nodes.
			flag := t.newFlag()
			return true, &shortNode{concat(n.Key, child.Key...), child.Val, flag}, nil
		default: // fullNode
			return true, &shortNode{n.Key, child, t.newFlag()}, nil
		}

	case *fullNode:
		dirty, nn, err := t.delete(n.Children[key[0]], append(prefix, key[0]), key[1:])
		if !dirty || err != nil {
			return false, n, err
		}

		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn

		// Check how many non-nil entries are left after deleting and
		// reduce the full node to a short node if only one entry is
		// left. Since n must've contained at least two children
		// before deletion (otherwise it would not be a full node) n
		// can never be reduced to nil.
		//
		// When the loop is done, pos contains the index of the single
		// value that is left in n or -2 if n contains at least two
		// values.
		pos := -1
		for i, cld := range n.Children {
			if cld != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if pos >= 0 {
			if pos != 16 {
				// If the remaining entry is a short node, it replaces
				// n and its key gets the missing nibble tacked to the
				// front. This avoids creating an invalid
				// shortNode{..., shortNode{...}}.  Since the entry
				// might not be loaded yet, resolve it just for this
				// check.
				cnode, err := t.resolve(n.Children[pos], append(prefix, byte(pos)))
				if err != nil {
					return false, nil, err
				}
				if cnode, ok := cnode.(*shortNode); ok {
					k := append([]byte{byte(pos)}, cnode.Key...)
					flag := t.newFlag()
					return true, &shortNode{k, cnode.Val, flag}, nil
				}
			}
			// Otherwise, n is replaced by a one-nibble short node
			// containing the child.
			flag := t.newFlag()
			return true, &shortNode{[]byte{byte(pos)}, n.Children[pos], flag}, nil
		}
		// n still contains at least two values and cannot be reduced.
		return true, n, nil

	case *valueNode:
		return true, nil, nil

	case nil:
		return false, nil, nil

	case *hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, _, err := t.resolveHash(n, prefix, false)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.delete(rn, prefix, key)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key))
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolve(n node, prefix []byte) (node, error) {
	if n, ok := n.(*hashNode); ok {
		node, _, err := t.resolveHash(n, prefix, false)
		return node, err
	}
	return n, nil
}

func (t *Trie) resolveHash(n *hashNode, prefix []byte, scaning bool) (node, []byte, error) {
	if ex, ok := t.db.(DatabaseReaderEx); ok {
		key := CompositKey{
			n.hash,
			n.ver,
			prefix,
			scaning,
		}

		enc, err := ex.GetEx(&key)
		if err != nil || enc == nil {
			fmt.Println(n.ver)
			debug.PrintStack()
			return nil, nil, &MissingNodeError{NodeHash: thor.BytesToBytes32(n.hash), Path: prefix, Err: err}
		}

		encNode, encVers, metaList, err := enc.Split()
		if err != nil {
			panic(fmt.Sprintf("split node %x: %v", n.hash, err))
		}

		dec := mustDecodeNode(n, encNode, encVers, metaList)
		return dec, enc, nil
	}

	enc, err := t.db.Get(n.hash)
	if err != nil || enc == nil {
		return nil, nil, &MissingNodeError{NodeHash: thor.BytesToBytes32(n.hash), Path: prefix, Err: err}
	}
	return mustDecodeNode(n, enc, nil, nil), enc, nil
}

// Root returns the root hash of the trie.
// Deprecated: use Hash instead.
func (t *Trie) Root() []byte { return t.Hash().Bytes() }

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *Trie) Hash() thor.Bytes32 {
	hash, cached, _ := t.hashRoot(nil, 0)
	t.root = cached
	return thor.BytesToBytes32(hash.(*hashNode).hash)
}

// Commit writes all nodes to the trie's database.
// Nodes are stored with their blake2b hash as the key.
//
// Committing flushes nodes from memory.
// Subsequent Get calls will load nodes from the database.
func (t *Trie) Commit() (root thor.Bytes32, err error) {
	return t.CommitVersioned(0)
}

// CommitTo writes all nodes to the given database.
// Nodes are stored with their blake2b hash as the key.
//
// Committing flushes nodes from memory. Subsequent Get calls will
// load nodes from the trie's database. Calling code must ensure that
// the changes made to db are written back to the trie's attached
// database before using the trie.
func (t *Trie) CommitTo(db DatabaseWriter) (root thor.Bytes32, err error) {
	return t.CommitVersionedTo(db, 0)
}

// Commit writes all nodes to the trie's database.
// Nodes are stored with their blake2b hash as the key.
//
// Committing flushes nodes from memory.
// Subsequent Get calls will load nodes from the database.
func (t *Trie) CommitVersioned(newVer uint32) (root thor.Bytes32, err error) {
	if t.db == nil {
		panic("Commit called on trie with nil database")
	}
	return t.CommitVersionedTo(t.db, newVer)
}

// CommitTo writes all nodes to the given database.
// Nodes are stored with their blake2b hash as the key.
//
// Committing flushes nodes from memory. Subsequent Get calls will
// load nodes from the trie's database. Calling code must ensure that
// the changes made to db are written back to the trie's attached
// database before using the trie.
func (t *Trie) CommitVersionedTo(db DatabaseWriter, newVer uint32) (root thor.Bytes32, err error) {
	hash, cached, err := t.hashRoot(db, newVer)
	if err != nil {
		return (thor.Bytes32{}), err
	}

	t.root = cached
	return thor.BytesToBytes32(hash.(*hashNode).hash), nil
}

func (t *Trie) hashRoot(db DatabaseWriter, newVer uint32) (node, node, error) {
	if t.root == nil {
		return &hashNode{hash: emptyRoot.Bytes()}, nil, nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	hashed, cached, _, err := h.hash(t.root, db, nil, true, newVer)
	return hashed, cached, err
}
