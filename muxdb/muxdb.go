package muxdb

import (
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

var (
	trieSlot          = byte(0)
	trieSecureKeySlot = byte(1)
	freeSlot          = byte(2)
)

type MuxDB struct {
	engine engine
	cache  *cache
}

func New(path string, options *Options) (*MuxDB, error) {
	db, err := openLevelDB(path, options.CacheSize/4, options.FileDescriptorCache)
	if err != nil {
		return nil, err
	}

	return &MuxDB{
		db,
		newCache(options.CacheSize * 3 / 4),
	}, nil
}

func NewMem() (*MuxDB, error) {
	db, err := openMemDB()
	if err != nil {
		return nil, err
	}
	return &MuxDB{
		db,
		nil,
	}, nil
}

func (m *MuxDB) NewTrie(root thor.Bytes32, blockNum uint32, secure bool) Trie {
	var (
		rSeg = segment((blockNum - 1) >> 16)
		wSeg = segment(blockNum >> 16)
		bkt  = bucket{trieSlot}
	)

	raw, err := trie.New(root, struct {
		getFunc
		hasFunc
		putFunc
	}{
		m.cache.ProxyGet(func(key []byte) ([]byte, error) {
			var val []byte
			if err := m.engine.Snapshot(func(getter Getter) error {
				v, err := rSeg.ProxyGet(bkt.ProxyGet(getter.Get))(key)
				if err != nil {
					return err
				}
				val = v
				return nil
			}); err != nil {
				return nil, err
			}
			return val, nil
		}),
		nil,
		nil,
	})

	if secure {
		var (
			hasher             = thor.NewBlake2b()
			hash               thor.Bytes32
			secureKeyPreimages map[thor.Bytes32][]byte
		)
		return &trieWrap{
			raw,
			err,
			func(key []byte, save bool) []byte {
				hasher.Reset()
				hasher.Write(key)
				hasher.Sum(hash[:0])
				if save {
					if secureKeyPreimages == nil {
						secureKeyPreimages = make(map[thor.Bytes32][]byte)
					}
					secureKeyPreimages[hash] = key
				}
				return hash[:]
			},
			func(fn func(putFunc) error) error {
				return m.engine.Batch(func(putter Putter) error {
					saveSecureKey := bucket{trieSecureKeySlot}.ProxyPut(putter.Put)
					for h, p := range secureKeyPreimages {
						if err := saveSecureKey(h[:], p); err != nil {
							return err
						}
					}
					secureKeyPreimages = nil
					return fn(m.cache.ProxyPut(wSeg.ProxyPut(bkt.ProxyPut(putter.Put))))
				})
			},
		}
	}

	return &trieWrap{
		raw,
		err,
		func(key []byte, save bool) []byte { return key },
		func(fn func(putFunc) error) error {
			return m.engine.Batch(func(putter Putter) error {
				return fn(m.cache.ProxyPut(wSeg.ProxyPut(bkt.ProxyPut(putter.Put))))
			})
		},
	}
}

func (m *MuxDB) NewBucket(name string) KV {
	bucket := append(bucket{freeSlot}, []byte(name)...)
	return bucket.ProxyKV(m.engine)
}

func (m *MuxDB) GetSecureKey(hash thor.Bytes32) ([]byte, error) {
	return bucket{trieSecureKeySlot}.ProxyGet(m.engine.Get)(hash[:])
}

func (m *MuxDB) Close() error {
	return m.engine.Close()
}
