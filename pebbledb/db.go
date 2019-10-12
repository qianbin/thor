package pebbledb

import (
	"github.com/cockroachdb/pebble"
	"github.com/vechain/thor/kv"
)

var _ kv.GetPutCloser = (*PebbleDB)(nil)

type PebbleDB struct {
	db *pebble.DB
}

func New(path string) (*PebbleDB, error) {
	db, err := pebble.Open(path, (&pebble.Options{}).EnsureDefaults())
	if err != nil {
		return nil, err
	}
	return &PebbleDB{db}, nil
}

func (p *PebbleDB) Get(key []byte) ([]byte, error) {
	return p.db.Get(key)
}

func (p *PebbleDB) IsNotFound(err error) bool {
	return err == pebble.ErrNotFound
}

func (p *PebbleDB) Has(key []byte) (bool, error) {
	_, err := p.Get(key)
	if err != nil {
		if p.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (p *PebbleDB) Put(key, value []byte) error {
	return p.db.Set(key, value, pebble.NoSync)
}

func (p *PebbleDB) Delete(key []byte) error {
	return p.db.Delete(key, pebble.NoSync)
}

func (p *PebbleDB) Close() error {
	return p.db.Close()
}

func (p *PebbleDB) NewBatch() kv.Batch {
	return &batch{p, p.db.NewBatch()}
}

func (p *PebbleDB) NewIterator(r kv.Range) kv.Iterator {
	it := p.db.NewIter(&pebble.IterOptions{
		LowerBound:  r.From,
		UpperBound:  r.To,
		TableFilter: func(userProps map[string]string) bool { return true },
	})
	it.First()
	return &iter{it}
}

type batch struct {
	p *PebbleDB
	b *pebble.Batch
}

func (b *batch) Put(key, value []byte) error {
	return b.b.Set(key, value, pebble.NoSync)
}

func (b *batch) Delete(key []byte) error {
	return b.b.Delete(key, pebble.NoSync)
}

func (b *batch) NewBatch() kv.Batch {
	return b.p.NewBatch()
}

func (b *batch) Len() int {
	return int(b.b.Count())
}
func (b *batch) Write() error {
	return b.b.Commit(pebble.NoSync)
}

type iter struct {
	it *pebble.Iterator
}

func (i *iter) Next() bool {
	return i.it.Next()
}
func (i *iter) Release() {
	i.it.Close()
}

func (i *iter) Error() error {
	return i.it.Error()
}

func (i *iter) Key() []byte {
	return i.it.Key()
}
func (i *iter) Value() []byte {
	return i.it.Value()
}
