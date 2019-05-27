package boltdb

import (
	"bytes"
	"errors"

	"github.com/vechain/thor/kv"
	"go.etcd.io/bbolt"
)

var (
	_             kv.GetPutCloser = (*BoltDB)(nil)
	defaultBucket                 = []byte("_")
	errNotFound                   = errors.New("not found")
)

type BoltDB struct {
	db *bbolt.DB
}

func New(path string) (*BoltDB, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{NoSync: true})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &BoltDB{db}, nil
}

func (bdb *BoltDB) IsNotFound(err error) bool {
	return err == errNotFound
}

func (bdb *BoltDB) Get(key []byte) (value []byte, err error) {
	err = bdb.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(defaultBucket).Get(key)
		if v == nil {
			return errNotFound
		}
		value = append([]byte(nil), v...)
		return nil
	})
	return
}

func (bdb *BoltDB) Has(key []byte) (exists bool, err error) {
	err = bdb.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(defaultBucket).Get(key)
		exists = v != nil
		return nil
	})
	return
}
func (bdb *BoltDB) Put(key, value []byte) error {
	return bdb.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(defaultBucket).Put(key, value)
	})
}

func (bdb *BoltDB) Delete(key []byte) error {
	return bdb.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		if bucket.Get(key) == nil {
			return errNotFound
		}
		return bucket.Delete(key)
	})
}

func (bdb *BoltDB) Close() error {
	return bdb.db.Close()
}

func (bdb *BoltDB) NewBatch() kv.Batch {
	return &batch{db: bdb.db}
}
func (bdb *BoltDB) NewIterator(r kv.Range) kv.Iterator {
	return newIterator(bdb.db, r)
}

type batch struct {
	db  *bbolt.DB
	ops []func(bucket *bbolt.Bucket) error
}

func (b *batch) Put(key, value []byte) error {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)
	b.ops = append(b.ops, func(bucket *bbolt.Bucket) error {
		return bucket.Put(key, value)
	})
	return nil
}

func (b *batch) Delete(key []byte) error {
	key = append([]byte(nil), key...)
	b.ops = append(b.ops, func(bucket *bbolt.Bucket) error {
		return bucket.Delete(key)
	})
	return nil
}

func (b *batch) NewBatch() kv.Batch {
	return &batch{db: b.db}
}

func (b *batch) Len() int {
	return len(b.ops)
}

func (b *batch) Write() error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		for _, op := range b.ops {
			if err := op(bucket); err != nil {
				return err
			}
		}
		return nil
	})
}

type iterator struct {
	r          kv.Range
	tx         *bbolt.Tx
	cursor     *bbolt.Cursor
	err        error
	key, value []byte
}

func newIterator(db *bbolt.DB, r kv.Range) *iterator {
	tx, err := db.Begin(false)
	if err != nil {
		return &iterator{err: err}
	}

	return &iterator{
		r:  r,
		tx: tx,
	}
}

func (i *iterator) Next() bool {
	if i.tx == nil {
		return false
	}
	if i.cursor == nil {
		i.cursor = i.tx.Bucket(defaultBucket).Cursor()
		i.key, i.value = i.cursor.Seek(i.r.From)
	} else {
		i.key, i.value = i.cursor.Next()
	}
	if i.key == nil {
		return false
	}
	return bytes.Compare(i.key, i.r.To) <= 0
}

func (i *iterator) Release() {
	if i.tx == nil {
		return
	}
	i.tx.Rollback()
}

func (i *iterator) Error() error {
	return i.err
}

func (i *iterator) Key() []byte {
	if i.err != nil {
		return nil
	}
	return append([]byte(nil), i.key...)
}

func (i *iterator) Value() []byte {
	if i.err != nil {
		return nil
	}
	return append([]byte(nil), i.value...)
}
