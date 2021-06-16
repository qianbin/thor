package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

type dbex struct {
	trie.Database
}

func (d *dbex) Get(key []byte) ([]byte, error) {
	panic("unexpected call")
}

func (d *dbex) Has(key []byte) (bool, error) {
	panic("unexpected call")
}

func (d *dbex) Put(key, val []byte) error {
	panic("unexpected call")
}

func (d *dbex) PutEx(key *trie.CompositKey, val trie.CompositValue) error {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], key.Ver)
	k := append(prefix[:], key.Hash...)

	return d.Database.Put(k, val)
}

func (d *dbex) GetEx(key *trie.CompositKey) (trie.CompositValue, error) {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], key.Ver)
	k := append(prefix[:], key.Hash...)

	return d.Database.Get(k)
}

func genKV(i int) (k, v []byte) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i))
	k = thor.Blake2b(b[:]).Bytes()
	v = thor.Blake2b(k).Bytes()
	return
}

func memdbValueBytes(db *ethdb.MemDatabase) int {
	n := 0
	for _, k := range db.Keys() {
		val, _ := db.Get(k)
		n += len(val)
	}
	return n
}

func pruneTrie(nVer, kvPerVer, nPruneVer int) {
	roots := make(map[int]thor.Bytes32, nVer)
	memdb1 := ethdb.NewMemDatabase()
	memdb2 := ethdb.NewMemDatabase()
	db1 := &dbex{memdb1}
	db2 := &dbex{memdb2}
	var root1, root2 thor.Bytes32
	{
		tr1, _ := trie.New(thor.Bytes32{}, db1)
		tr1.Update([]byte("foo"), []byte("bar"))
		tr1.Update([]byte("hello"), []byte("world"))
		root1, _ = tr1.Commit()
		// tr2, _ := trie.NewVersioned(root1, db1, 1)
		// tr2.Delete([]byte("hello"))
		// root2, _ = tr2.CommitVersioned(2)
	}

	tr1, _ := trie.New(root1, db1)
	// tr2, _ := trie.NewVersioned(root2, db1, 2)

	// it, _ := trie.NewDifferenceIterator(tr1.NodeIterator(nil), tr2.NodeIterator(nil))
	it := tr1.NodeIterator(nil)
	_ = root2
	for it.Next(true) {
		fmt.Println(it.Path())
	}
	return

	var err error

	for ver := 0; ver < nVer; ver++ {
		tr, _ := trie.NewVersioned(roots[ver-1], db1, uint32(ver-1))
		for i := 0; i < kvPerVer; i++ {
			k, v := genKV(ver*kvPerVer + i)
			if err := tr.TryUpdate(k, v); err != nil {
				panic(err)
			}
		}
		roots[ver], err = tr.CommitVersioned(uint32(ver))
		if err != nil {
			panic(err)
		}
	}

	tr, _ := trie.NewVersioned(roots[nVer-1], db1, uint32(nVer-1))
	for i := 0; i < kvPerVer*nVer; i++ {
		k, v := genKV(i)

		got, err := tr.TryGet(k)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(got, v) {
			panic("incorrect value")
		}
	}

	prevVer := -1
	curVer := nPruneVer
	for {
		tr1, _ := trie.NewVersioned(roots[prevVer], db1, uint32(prevVer))
		tr2, _ := trie.NewVersioned(roots[curVer], db1, uint32(curVer))

		diff, _ := trie.NewDifferenceIterator(tr1.NodeIterator(nil), tr2.NodeIterator(nil))
		for diff.Next(true) {
			if h := diff.Hash(); !h.IsZero() {
				diff.Node(func(val []byte) error {
					return db2.PutEx(&trie.CompositKey{
						Hash:    h.Bytes(),
						Ver:     diff.Ver(),
						Path:    diff.Path(),
						Scaning: false,
					}, val)
				})
			}
		}
		if err := diff.Error(); err != nil {
			panic(err)
		}
		if curVer == nVer-1 {
			break
		}

		curVer += nPruneVer
		prevVer += nPruneVer
		if curVer >= nVer {
			curVer = nVer - 1
		}
	}

	ptr, err := trie.NewVersioned(roots[nVer-1], db2, uint32(nVer-1))
	if err != nil {
		panic(err)
	}
	for ver := 0; ver < nVer; ver++ {
		k, v := genKV(ver)
		_v, err := ptr.TryGet(k)
		if err != nil {
			panic(fmt.Sprintf("%v %v", ver, err))
		}
		if !bytes.Equal(v, _v) {
			panic("incorrect value")
		}
	}

	len1, len2 := memdb1.Len(), memdb2.Len()
	size1, size2 := memdbValueBytes(memdb1), memdbValueBytes(memdb2)
	lenRatio := float64(len2) * 100 / float64(len1)
	sizeRatio := float64(size2) * 100 / float64(size1)

	fmt.Printf("%v-%v-%v, original: %v|%v, pruned: %v|%v, ratio: %.2f%%|%.2f%%\n",
		nVer, kvPerVer, nPruneVer,
		memdb1.Len(), memdbValueBytes(memdb1),
		memdb2.Len(), memdbValueBytes(memdb2),
		lenRatio, sizeRatio,
	)

}

func main() {
	pruneTrie(2000, 20, 100)
	pruneTrie(2000, 20, 200)
	pruneTrie(2000, 20, 300)
	pruneTrie(2000, 20, 400)
	pruneTrie(2000, 20, 500)
}
