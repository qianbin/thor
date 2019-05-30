package boltdb

import (
	"errors"

	"github.com/vechain/thor/thor"

	"github.com/vechain/thor/kv"
	"go.etcd.io/bbolt"
)

var (
	_           kv.GetPutCloser = (*BoltDB)(nil)
	errNotFound                 = errors.New("not found")
)

type BoltDB struct {
	db *bbolt.DB
	// j1      *journal
	// j2      *journal
	// lock    sync.Mutex
	// goes    co.Goes
	// flushCh chan struct{}
}

func New(path string) (*BoltDB, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{NoSync: true})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		for i := 0; i < 4096; i++ {
			if _, err := tx.CreateBucketIfNotExists([]byte{byte(i % 256), byte(i / 256)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	bdb := &BoltDB{db: db /* flushCh: make(chan struct{})*/}

	// bdb.goes.Go(bdb.journalWriter)
	return bdb, nil
}

// func (bdb *BoltDB) journalWriter() {
// 	ticker := time.NewTicker(time.Second * 2)
// 	defer ticker.Stop()

// 	end := false
// 	for !end {
// 		select {
// 		case <-ticker.C:
// 		case <-bdb.flushCh:
// 			end = true
// 		}

// 		bdb.lock.Lock()
// 		if bdb.j2 == nil {
// 			bdb.j2 = bdb.j1
// 			bdb.j1 = nil
// 		}
// 		bdb.lock.Unlock()
// 		j := bdb.j2
// 		if j != nil {
// 			if err := bdb.db.Update(func(tx *bbolt.Tx) error {
// 				bucket := tx.Bucket(defaultBucket)
// 				for _, a := range j.actions {
// 					if a.value == nil {
// 						if err := bucket.Delete(a.key); err != nil {
// 							return err
// 						}
// 					} else {
// 						if err := bucket.Put(a.key, *a.value); err != nil {
// 							return err
// 						}
// 					}
// 				}
// 				return nil
// 			}); err != nil {
// 				fmt.Println(err)
// 			} else {
// 				bdb.lock.Lock()
// 				bdb.j2 = nil
// 				bdb.lock.Unlock()
// 				fmt.Println(j.Len())
// 			}
// 		}
// 	}
// }

func (bdb *BoltDB) IsNotFound(err error) bool {
	return err == errNotFound
}

// func (bdb *BoltDB) getFromJournal(key []byte) *entry {

// 	if bdb.j1 != nil {
// 		ent := bdb.j1.Get(key)
// 		if ent != nil {
// 			return ent
// 		}
// 	}
// 	if bdb.j2 != nil {
// 		ent := bdb.j2.Get(key)
// 		if ent != nil {
// 			return ent
// 		}
// 	}
// 	return nil
// }

func (bdb *BoltDB) Get(key []byte) (value []byte, err error) {
	// bdb.lock.Lock()
	// ent := bdb.getFromJournal(key)
	// bdb.lock.Unlock()
	// if ent != nil {
	// 	if ent.deleted {
	// 		return nil, errNotFound
	// 	} else {
	// 		return append([]byte(nil), ent.value...), nil
	// 	}
	// }

	err = bdb.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketOfKey(key)).Get(key)
		if v == nil {
			return errNotFound
		}
		value = append([]byte(nil), v...)
		return nil
	})
	return
}

func (bdb *BoltDB) Has(key []byte) (exists bool, err error) {
	// ent := bdb.getFromJournal(key)
	// if ent != nil {
	// 	return !ent.deleted, nil
	// }

	err = bdb.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketOfKey(key)).Get(key)
		exists = v != nil
		return nil
	})
	return
}

// func (bdb *BoltDB) putJournal(key []byte, value *[]byte) {
// 	if bdb.j1 == nil {
// 		bdb.j1 = newJournal()
// 	}

// 	if value == nil {
// 		bdb.j1.Delete(key)
// 	} else {
// 		bdb.j1.Put(key, *value)
// 	}
// }

func (bdb *BoltDB) Put(key, value []byte) error {
	// bdb.lock.Lock()
	// bdb.putJournal(key, &value)
	// bdb.lock.Unlock()
	return bdb.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(bucketOfKey(key))
		bkt.FillPercent = 0.2
		return bkt.Put(key, value)
	})

}

func (bdb *BoltDB) Delete(key []byte) error {
	// bdb.putJournal(key, nil)
	return bdb.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketOfKey(key))
		bucket.FillPercent = 0.2
		return bucket.Delete(key)
	})
	// return nil
}

func (bdb *BoltDB) Close() error {
	// close(bdb.flushCh)
	// bdb.goes.Wait()
	return bdb.db.Close()
}

func (bdb *BoltDB) NewBatch() kv.Batch {
	return &batch{db: bdb}
}

func (bdb *BoltDB) NewIterator(r kv.Range) kv.Iterator {
	panic("not implemented")
	// return newIterator(bdb.db, r)
}

type batch struct {
	db      *BoltDB
	actions []*action
}

func (b *batch) Put(key, value []byte) error {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)
	b.actions = append(b.actions, &action{
		key, &value,
	})
	return nil
}

func (b *batch) Delete(key []byte) error {
	key = append([]byte(nil), key...)
	b.actions = append(b.actions, &action{
		key, nil,
	})
	return nil
}

func (b *batch) NewBatch() kv.Batch {
	return &batch{db: b.db}
}

func (b *batch) Len() int {
	return len(b.actions)
}

func (b *batch) Write() error {
	return b.db.db.Update(func(tx *bbolt.Tx) error {
		for _, a := range b.actions {
			bkt := tx.Bucket(bucketOfKey(a.key))
			bkt.FillPercent = 0.2
			if a.value == nil {
				if err := bkt.Delete(a.key); err != nil {
					return err
				}
			} else {
				if err := bkt.Put(a.key, *a.value); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// b.db.lock.Lock()
	// defer b.db.lock.Unlock()

	// for _, a := range b.actions {
	// 	b.db.putJournal(a.key, a.value)
	// }
	// return nil
}

// type iterator struct {
// 	r          kv.Range
// 	tx         *bbolt.Tx
// 	cursor     *bbolt.Cursor
// 	err        error
// 	key, value []byte
// }

// func newIterator(db *bbolt.DB, r kv.Range) *iterator {
// 	tx, err := db.Begin(false)
// 	if err != nil {
// 		return &iterator{err: err}
// 	}

// 	return &iterator{
// 		r:  r,
// 		tx: tx,
// 	}
// }

// func (i *iterator) Next() bool {
// 	if i.tx == nil {
// 		return false
// 	}
// 	if i.cursor == nil {
// 		i.cursor = i.tx.Bucket(defaultBucket).Cursor()
// 		i.key, i.value = i.cursor.Seek(i.r.From)
// 	} else {
// 		i.key, i.value = i.cursor.Next()
// 	}
// 	if i.key == nil {
// 		return false
// 	}
// 	return bytes.Compare(i.key, i.r.To) <= 0
// }

// func (i *iterator) Release() {
// 	if i.tx == nil {
// 		return
// 	}
// 	i.tx.Rollback()
// }

// func (i *iterator) Error() error {
// 	return i.err
// }

// func (i *iterator) Key() []byte {
// 	if i.err != nil {
// 		return nil
// 	}
// 	return append([]byte(nil), i.key...)
// }

// func (i *iterator) Value() []byte {
// 	if i.err != nil {
// 		return nil
// 	}
// 	return append([]byte(nil), i.value...)
// }

func bucketOfKey(key []byte) []byte {
	h := thor.Blake2b(key)
	return []byte{h[0], h[1] >> 4}
}
