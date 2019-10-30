package muxdb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

var (
	trieSlot          = byte(0)
	trieSecureKeySlot = byte(1)
	freeSlot          = byte(2)

	dynamicSpots = [2]byte{0, 1}
	matureSpot   = byte(2)
)

type MuxDB struct {
	engine    engine
	trieSpots trix
	cache     *cache
	lock      sync.Mutex
}

func New(path string, options *Options) (*MuxDB, error) {
	db, err := openLevelDB(path, options.CacheSize/4, options.FileDescriptorCache)
	if err != nil {
		return nil, err
	}
	c := newCache(options.CacheSize * 3 / 4)

	go func() {
		for {
			time.Sleep(time.Minute)
			fmt.Println(
				c.fc.EntryCount(),
				c.fc.HitCount(),
				c.fc.MissCount(),
				c.fc.EvacuateCount(),
				c.fc.HitRate())
		}
	}()
	return &MuxDB{
		engine:    db,
		trieSpots: trix{dynamicSpots[0], dynamicSpots[1], matureSpot},
		cache:     c,
	}, nil
}

func NewMem() (*MuxDB, error) {
	db, err := openMemDB()
	if err != nil {
		return nil, err
	}
	return &MuxDB{
		engine:    db,
		trieSpots: trix{dynamicSpots[0], dynamicSpots[1], matureSpot},
		cache:     nil,
	}, nil
}
func makeHashKey(hash []byte, path []byte) []byte {
	var key [36]byte
	for i := 0; i < len(path) && i < 8; i++ {
		if i%2 == 0 {
			key[i/2] |= path[i] << 4
		} else {
			key[i/2] |= path[i]
		}
	}
	copy(key[4:], hash)
	return key[:]
}

func (m *MuxDB) NewTrie(name string, root thor.Bytes32, secure bool) Trie {
	return m.newTrie(name, root, secure, false)
}

func (m *MuxDB) NewTrieNoFillCache(name string, root thor.Bytes32, secure bool) Trie {
	return m.newTrie(name, root, secure, true)
}

func (m *MuxDB) newTrie(name string, root thor.Bytes32, secure bool, dontFillCache bool) Trie {

	bkt := append(bucket{trieSlot}, []byte(name)...)
	m.lock.Lock()
	t := m.trieSpots
	m.lock.Unlock()

	var path []byte
	raw, err := trie.New(root, struct {
		getFunc
		hasFunc
		putFunc
		pathFunc
	}{
		m.cache.ProxyGet(func(key []byte) ([]byte, error) {
			var val []byte
			if err := m.engine.Snapshot(func(getter kv.Getter) error {
				v, err := t.ProxyGet(bkt.ProxyGet(getter.Get))(makeHashKey(key, path))
				if err != nil {
					return err
				}
				val = v
				return nil
			}); err != nil {
				return nil, err
			}
			return val, nil
		}, dontFillCache),
		nil,
		nil,
		func(p []byte) {
			path = p
		},
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
				return m.engine.Batch(func(putter kv.Putter) error {
					saveSecureKey := bucket{trieSecureKeySlot}.ProxyPut(putter.Put)
					for h, p := range secureKeyPreimages {
						if err := saveSecureKey(h[:], p); err != nil {
							return err
						}
					}
					secureKeyPreimages = nil
					x := t.ProxyPut(bkt.ProxyPut(putter.Put))
					return fn(m.cache.ProxyPut(func(k, v []byte) error {
						return x(makeHashKey(k, path), v)
					}))
				})
			},
			func(p []byte) {
				path = p
			},
		}
	}

	return &trieWrap{
		raw,
		err,
		func(key []byte, save bool) []byte { return key },
		func(fn func(putFunc) error) error {
			return m.engine.Batch(func(putter kv.Putter) error {
				x := t.ProxyPut(bkt.ProxyPut(putter.Put))
				return fn(m.cache.ProxyPut(func(k, v []byte) error {
					return x(makeHashKey(k, path), v)
				}))
			})
		},
		func(p []byte) {
			path = p
		},
	}
}

func (m *MuxDB) RollTrie(name string, i int) ([]byte, []byte, []byte) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.trieSpots = trix{dynamicSpots[i%2], dynamicSpots[(i+1)%2], matureSpot}

	return append(append([]byte{trieSlot}, []byte(name)...), dynamicSpots[i%2]),
		append(append([]byte{trieSlot}, []byte(name)...), dynamicSpots[(i+1)%2]),
		append(append([]byte{trieSlot}, []byte(name)...), matureSpot)
}

func (m *MuxDB) NewStore(name string, doCache bool) kv.Store {
	cache := m.cache
	if !doCache {
		cache = nil
	}
	b := append(bucket{freeSlot}, []byte(name)...)

	return &struct {
		kv.Getter
		kv.Putter
		snapshotFunc
		batchFunc
		iterateFunc
		isNotFoundFunc
		compactFunc
	}{
		cache.ProxyGetter(b.ProxyGetter(m.engine)),
		cache.ProxyPutter(b.ProxyPutter(m.engine)),
		func(fn func(kv.Getter) error) error {
			return m.engine.Snapshot(func(getter kv.Getter) error {
				return fn(cache.ProxyGetter(b.ProxyGetter(getter)))
			})
		},
		func(fn func(kv.Putter) error) error {
			return m.engine.Batch(func(putter kv.Putter) error {
				return fn(cache.ProxyPutter(b.ProxyPutter(putter)))
			})
		},
		func(prefix []byte, fn func([]byte, []byte) error) error {
			bucketLen := len(b)
			return m.engine.Iterate(append(b, prefix...), func(key, val []byte) error {
				return fn(key[bucketLen:], val)
			})
		},
		m.engine.IsNotFound,
		func([]byte, []byte) error { return errors.New("not supported") },
	}
}

func (m *MuxDB) LowStore() kv.Store {
	return m.engine
}

func (m *MuxDB) GetSecureKey(hash thor.Bytes32) ([]byte, error) {
	return bucket{trieSecureKeySlot}.ProxyGet(m.engine.Get)(hash[:])
}

func (m *MuxDB) Close() error {
	return m.engine.Close()
}

func (m *MuxDB) IsNotFound(err error) bool {
	return m.engine.IsNotFound(err)
}
