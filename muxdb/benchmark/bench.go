package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

const (
	rootKey       = "root"
	totalKeyCount = 5000000
	readKeyCount  = 5000
	iterateCount  = 10000
)

type bench struct {
	path      string
	optimized bool
}

func (b *bench) openDB() (*muxdb.MuxDB, error) {
	return muxdb.Open(b.path, &muxdb.Options{
		DisablePageCache:       true,
		OpenFilesCacheCapacity: 500,
		ReadCacheMB:            256,
		WriteBufferMB:          128,
	})
}

func (b *bench) Write(f func(put kv.PutFunc) error) error {
	db, err := b.openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	root, v, err := loadRoot(db)
	if err != nil {
		return err
	}

	if !root.IsZero() {
		return nil
	}
	if b.optimized {
		tr := db.NewTrie("", thor.Bytes32{}, 0)
		count := 0

		if err := f(func(key, val []byte) error {
			if err := tr.Update(key, val); err != nil {
				return err
			}
			if count > 0 && count%10000 == 0 {
				v++
				if _, err := tr.CommitVer(v / 10); err != nil {
					return err
				}
			}
			count++
			return nil
		}); err != nil {
			return err
		}
		v++
		if root, err = tr.CommitVer(v / 10); err != nil {
			return err
		}
	} else {
		tr, err := trie.New(thor.Bytes32{}, db.LowStore())
		if err != nil {
			return err
		}
		count := 0

		if err := f(func(key, val []byte) error {
			if err := tr.TryUpdate(key, val); err != nil {
				return err
			}
			if count > 0 && count%10000 == 0 {
				if _, err := tr.Commit(); err != nil {
					return err
				}
			}
			count++
			return nil
		}); err != nil {
			return err
		}
		if root, err = tr.Commit(); err != nil {
			return err
		}
	}
	return saveRoot(db, root, v/10)
}

func (b *bench) Read(f func(get kv.GetFunc) error) error {
	db, err := b.openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	root, v, err := loadRoot(db)
	if err != nil {
		return err
	}

	if b.optimized {
		return f(func(key []byte) ([]byte, error) {
			return db.NewTrie("", root, v).Get(key)
		})
	}

	return f(func(key []byte) ([]byte, error) {
		tr, err := trie.New(root, db.LowStore())
		if err != nil {
			return nil, err
		}
		return tr.TryGet(key)
	})
}

func (b *bench) Iterate(n int) error {
	db, err := b.openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	root, v, err := loadRoot(db)
	if err != nil {
		return err
	}

	var iter trie.NodeIterator
	if b.optimized {
		iter = db.NewTrie("", root, v).NodeIterator(nil)
	} else {
		tr, err := trie.New(root, db.LowStore())
		if err != nil {
			return err
		}
		iter = tr.NodeIterator(nil)
	}

	for i := 0; i < n && iter.Next(true); i++ {
	}
	return iter.Error()
}

func (b *bench) Run() error {
	fmt.Println("fill", totalKeyCount, "keys ...")
	t := time.Now().UnixNano()
	if err := b.Write(func(put kv.PutFunc) error {
		for i := 0; i < totalKeyCount; i++ {
			var b4 [4]byte
			binary.BigEndian.PutUint32(b4[:], uint32(i))
			key := thor.Blake2b(b4[:])
			value := thor.Blake2b(key[:])
			if err := put(key[:], value[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	fmt.Println("elapse:", time.Duration(time.Now().UnixNano()-t))

	fmt.Println("read", readKeyCount, "keys ...")
	t = time.Now().UnixNano()
	if err := b.Read(func(get kv.GetFunc) error {
		for i := 0; i < readKeyCount; i++ {
			var b4 [4]byte
			binary.BigEndian.PutUint32(b4[:], uint32(i))
			key := thor.Blake2b(b4[:])
			if _, err := get(key[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	fmt.Println("elapse:", time.Duration(time.Now().UnixNano()-t))

	fmt.Println("iterate", iterateCount, "nodes ...")
	t = time.Now().UnixNano()
	if err := b.Iterate(iterateCount); err != nil {
		return err
	}
	fmt.Println("elapse:", time.Duration(time.Now().UnixNano()-t))
	return nil
}

type vroot struct {
	V    uint32
	Root thor.Bytes32
}

func loadRoot(db *muxdb.MuxDB) (thor.Bytes32, uint32, error) {
	val, err := db.NewStore("c").Get([]byte(rootKey))
	if err != nil {
		if db.IsNotFound(err) {
			return thor.Bytes32{}, 0, nil
		}
		return thor.Bytes32{}, 0, err
	}
	var vr vroot
	rlp.DecodeBytes(val, &vr)
	return vr.Root, vr.V, nil
}

func saveRoot(db *muxdb.MuxDB, root thor.Bytes32, v uint32) error {
	enc, _ := rlp.EncodeToBytes(&vroot{
		v,
		root,
	})
	return db.NewStore("c").Put([]byte(rootKey), enc)
}
