package muxdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vechain/thor/kv"
)

var (
	writeOpt = opt.WriteOptions{}
	readOpt  = opt.ReadOptions{
		// DontTriggerCompaction: true,
	}
	scanOpt = opt.ReadOptions{
		DontFillCache: true,
	}

	_ engine = (*levelDB)(nil)
)

type levelDB struct {
	db *leveldb.DB
}

func openLevelDB(
	path string,
	cacheSize int,
	fileDescriptorCache int,
) (*levelDB, error) {

	if cacheSize < 16 {
		cacheSize = 16
	}

	if fileDescriptorCache < 16 {
		fileDescriptorCache = 16
	}

	opts := opt.Options{
		// CompactionTableSizeMultiplier: 2,
		OpenFilesCacheCapacity: fileDescriptorCache,
		BlockCacheCapacity:     256 * opt.MiB,
		// BlockSize:              256 * opt.KiB,
		//  CompactionL0Trigger:    32,
		WriteBuffer:            128 * opt.MiB, // Two of these are used internally
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
		// WriteL0PauseTrigger:    96,
		// WriteL0SlowdownTrigger: 64,
		// CompactionTotalSize:    80 * opt.MiB,
	}

	db, err := leveldb.OpenFile(path, &opts)

	if _, corrupted := err.(*dberrors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(path, &opts)
	}

	if err != nil {
		return nil, err
	}

	return &levelDB{db}, nil
}

func openSamll(path string) (*levelDB, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
	})

	if _, corrupted := err.(*dberrors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(path, nil)
	}

	if err != nil {
		return nil, err
	}

	return &levelDB{db}, nil
}

func openMemDB() (*levelDB, error) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}
	return &levelDB{db}, nil
}

func (l *levelDB) Close() error {
	return l.db.Close()
}

func (l *levelDB) IsNotFound(err error) bool {
	return err == leveldb.ErrNotFound
}

func (l *levelDB) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, &readOpt)
}

func (l *levelDB) Has(key []byte) (bool, error) {
	return l.db.Has(key, &readOpt)
}

func (l *levelDB) Put(key, val []byte) error {
	return l.db.Put(key, val, &writeOpt)
}

func (l *levelDB) Delete(key []byte) error {
	return l.db.Delete(key, &writeOpt)
}

func (l *levelDB) Snapshot(fn func(kv.Getter) error) error {
	s, err := l.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer s.Release()

	return fn(struct {
		getFunc
		hasFunc
	}{
		func(key []byte) ([]byte, error) {
			return s.Get(key, &readOpt)
		},
		func(key []byte) (bool, error) {
			return s.Has(key, &readOpt)
		},
	})
}

func (l *levelDB) Batch(fn func(kv.Putter) error) error {
	batch := &leveldb.Batch{}
	err := fn(struct {
		putFunc
		deleteFunc
	}{
		func(key, val []byte) error {
			batch.Put(key, val)
			return nil
		},
		func(key []byte) error {
			batch.Delete(key)
			return nil
		},
	})
	if err != nil {
		return err
	}
	return l.db.Write(batch, &writeOpt)
}

func (l *levelDB) Iterate(r kv.Range, fn func(key, val []byte) bool) error {
	it := l.db.NewIterator(&util.Range{
		Start: r.From,
		Limit: r.To,
	}, &scanOpt)
	defer it.Release()

	for it.Next() {
		if !fn(it.Key(), it.Value()) {
			break
		}
	}
	return it.Error()
}

func (l *levelDB) Compact(r kv.Range) error {
	return l.db.CompactRange(util.Range{Start: r.From, Limit: r.To})
}
