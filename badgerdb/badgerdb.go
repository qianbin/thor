package badgerdb

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/vechain/thor/kv"
)

var (
	_ kv.GetPutCloser = (*BadgerDB)(nil)
)

// BadgerDB wraps badger.
type BadgerDB struct {
	db *badger.DB
}

// New open badger
func New(path string) (*BadgerDB, error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.ValueLogLoadingMode = options.FileIO
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogFileSize = 64 << 20

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerDB{db}, nil
}

func (bdb *BadgerDB) IsNotFound(err error) bool {
	return err == badger.ErrKeyNotFound
}

func (bdb *BadgerDB) Get(key []byte) (value []byte, err error) {
	err = bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return
}

func (bdb *BadgerDB) Has(key []byte) (bool, error) {
	err := bdb.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err != nil {
		if bdb.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (bdb *BadgerDB) Put(key, value []byte) error {
	return bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (bdb *BadgerDB) Delete(key []byte) error {
	return bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (bdb *BadgerDB) Close() error {
	return bdb.db.Close()
}

func (bdb *BadgerDB) NewBatch() kv.Batch {
	return &batch{db: bdb.db}
}

func (bdb *BadgerDB) NewIterator(r kv.Range) kv.Iterator {
	panic("not implemented")
}

type batch struct {
	db      *badger.DB
	actions []func(txn *badger.Txn) error
}

func (b *batch) Put(key, value []byte) error {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)

	b.actions = append(b.actions, func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	return nil
}

func (b *batch) Delete(key []byte) error {
	key = append([]byte(nil), key...)
	b.actions = append(b.actions, func(txn *badger.Txn) error {
		return txn.Delete(key)
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
	txn := b.db.NewTransaction(true)
	for _, a := range b.actions {
		if err := a(txn); err == badger.ErrTxnTooBig {
			if err := txn.Commit(nil); err != nil {
				return err
			}
			txn = b.db.NewTransaction(true)
			if err := a(txn); err != nil {
				return err
			}
		}
	}
	return txn.Commit(nil)
}
