package pogrebdb

import (
	"errors"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/vechain/thor/kv"
)

var (
	_           kv.GetPutCloser = (*PogrebDB)(nil)
	errNotFound                 = errors.New("not found")
)

type PogrebDB struct {
	db *pogreb.DB
}

func New(path string) (*PogrebDB, error) {

	db, err := pogreb.Open(path, &pogreb.Options{BackgroundSyncInterval: time.Second})
	if err != nil {
		return nil, err
	}
	return &PogrebDB{db: db}, nil
}

func (pdb *PogrebDB) IsNotFound(err error) bool {
	return err == errNotFound
}

func (pdb *PogrebDB) Get(key []byte) ([]byte, error) {
	v, err := pdb.db.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errNotFound
	}
	return v, nil
}
func (pdb *PogrebDB) Has(key []byte) (bool, error) {
	return pdb.Has(key)
}

func (pdb *PogrebDB) Put(key, value []byte) error {
	return pdb.db.Put(key, value)
}

func (pdb *PogrebDB) Delete(key []byte) error {
	return pdb.db.Delete(key)
}

func (pdb *PogrebDB) Close() error {
	return pdb.db.Close()
}

func (pdb *PogrebDB) NewBatch() kv.Batch {
	return &batch{db: pdb.db}
}

func (pdb *PogrebDB) NewIterator(r kv.Range) kv.Iterator {
	panic("not implemented")
}

type batch struct {
	db      *pogreb.DB
	actions []func() error
}

func (b *batch) Put(key, value []byte) error {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)
	b.actions = append(b.actions, func() error {
		return b.db.Put(key, value)
	})
	return nil
}

func (b *batch) Delete(key []byte) error {
	key = append([]byte(nil), key...)
	b.actions = append(b.actions, func() error {
		return b.db.Delete(key)
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
	for _, a := range b.actions {
		if err := a(); err != nil {
			return err
		}
	}
	return nil
}
