package kv

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	writeOpt = opt.WriteOptions{}
	readOpt  = opt.ReadOptions{}
	scanOpt  = opt.ReadOptions{DontFillCache: true}
)

type levelEngine struct {
	db *leveldb.DB
}

// NewLevelEngine wraps leveldb instance to Engine interface.
func NewLevelEngine(db *leveldb.DB) Engine {
	return &levelEngine{db}
}

func (l *levelEngine) Close() error {
	return l.db.Close()
}

func (l *levelEngine) IsNotFound(err error) bool {
	return err == leveldb.ErrNotFound
}

func (l *levelEngine) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, &readOpt)
}

func (l *levelEngine) Has(key []byte) (bool, error) {
	return l.db.Has(key, &readOpt)
}

func (l *levelEngine) Put(key, val []byte) error {
	return l.db.Put(key, val, &writeOpt)
}

func (l *levelEngine) Delete(key []byte) error {
	return l.db.Delete(key, &writeOpt)
}

func (l *levelEngine) Snapshot(fn func(Getter) error) error {
	s, err := l.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer s.Release()

	return fn(struct {
		GetFunc
		HasFunc
	}{
		func(key []byte) ([]byte, error) { return s.Get(key, &readOpt) },
		func(key []byte) (bool, error) { return s.Has(key, &readOpt) },
	})
}

func (l *levelEngine) Batch(fn func(PutCommitter) error) error {
	batch := &leveldb.Batch{}

	if err := fn(struct {
		PutFunc
		DeleteFunc
		CommitFunc
	}{
		func(key, val []byte) error {
			batch.Put(key, val)
			return nil
		},
		func(key []byte) error {
			batch.Delete(key)
			return nil
		},
		func() error {
			err := l.db.Write(batch, &writeOpt)
			batch.Reset()
			return err
		},
	}); err != nil {
		return err
	}

	return l.db.Write(batch, &writeOpt)
}

func (l *levelEngine) Iterate(rng Range, fn func(Pair) bool) error {
	it := l.db.NewIterator((*util.Range)(&rng), &scanOpt)
	defer it.Release()

	for it.Next() {
		if !fn(it) {
			break
		}
	}
	return it.Error()
}
