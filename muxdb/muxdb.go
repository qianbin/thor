package muxdb

import (
	"context"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/vechain/thor/muxdb/kv"
	"github.com/vechain/thor/thor"
)

const (
	decTrieNodeCacheCount = 65536
	trieCommitSpaceA      = byte(0)
	trieCommitSpaceB      = byte(1)
	triePermanentSpace    = byte(15)
	trieSecureKeySpace    = byte(16)
	freeSpace             = byte(32)
)

type MuxDB struct {
	engine         kv.Engine
	trieCache      *trieCache
	triePruningSeq int32
}

func Open(path string, options *Options) (*MuxDB, error) {
	ldbOpts := opt.Options{
		CompactionTableSizeMultiplier: 2,
		OpenFilesCacheCapacity:        options.FDCache,
		BlockCacheCapacity:            256 * opt.MiB,
		WriteBuffer:                   64 * opt.MiB,
		Filter:                        filter.NewBloomFilter(10),
		DisableSeeksCompaction:        true,
	}
	ldb, err := leveldb.OpenFile(path, &ldbOpts)
	if _, corrupted := err.(*dberrors.ErrCorrupted); corrupted {
		ldb, err = leveldb.RecoverFile(path, &ldbOpts)
	}
	if err != nil {
		return nil, err
	}
	return &MuxDB{
		engine:    kv.NewLevelEngine(ldb),
		trieCache: newTrieCache(options.TrieCacheSizeMB, decTrieNodeCacheCount),
	}, nil
}

func OpenMem() (*MuxDB, error) {
	ldb, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}
	return &MuxDB{
		engine: kv.NewLevelEngine(ldb),
	}, nil
}

func (m *MuxDB) Close() error {
	return m.engine.Close()
}

func (m *MuxDB) NewTrie(name string, root thor.Bytes32, secure bool) (Trie, error) {
	return newManagedTrie(
		m.engine,
		m.trieCache,
		name,
		root,
		secure,
		func() byte {
			seq := triePruningSeq(atomic.LoadInt32(&m.triePruningSeq))
			return seq.CurrentCommitSpace()
		},
	)
}

func (m *MuxDB) NewTriePruner(ctx context.Context) *TriePruner {
	seq := triePruningSeq(atomic.AddInt32(&m.triePruningSeq, 1))
	return &TriePruner{
		ctx,
		m,
		seq.StaleCommitSpace(),
	}
}

func (m *MuxDB) NewStore(name string) kv.Store {
	bkt := bucket(append([]byte{freeSpace}, name...))
	return &struct {
		kv.Getter
		kv.Putter
		kv.SnapshotFunc
		kv.BatchFunc
		kv.IterateFunc
		kv.IsNotFoundFunc
	}{
		bkt.ProxyGetter(m.engine),
		bkt.ProxyPutter(m.engine),
		func(fn func(kv.Getter) error) error {
			return m.engine.Snapshot(func(getter kv.Getter) error {
				return fn(bkt.ProxyGetter(getter))
			})
		},
		func(fn func(kv.Putter) error) error {
			return m.engine.Batch(func(putter kv.Putter) error {
				return fn(bkt.ProxyPutter(putter))
			})
		},
		func(r kv.Range, fn func(kv.Pair) bool) error {
			return m.engine.Iterate(bkt.TransformRange(r), func(pair kv.Pair) bool {
				return fn(bkt.TransformPair(pair))
			})
		},
		m.engine.IsNotFound,
	}
}
