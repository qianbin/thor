package muxdb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
)

var (
	trieSlot          = byte(0)
	trieSecureKeySlot = byte(1)
	freeSlot          = byte(2)
)

type nodeCache struct {
	lru *lru.Cache
}

func (nc *nodeCache) Get(key []byte) interface{} {
	if nc == nil {
		return nil
	}
	v, _ := nc.lru.Get(string(key))
	return v
}
func (nc *nodeCache) Set(key []byte, val interface{}) {
	if nc == nil {
		return
	}
	nc.lru.Add(string(key), val)
}

type MuxDB struct {
	engine engine
	trix   trix
	cache  *cache
	lock   sync.Mutex

	nodeCache *nodeCache
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
	lru, _ := lru.New(65536)
	return &MuxDB{
		engine:    db,
		cache:     c,
		nodeCache: &nodeCache{lru},
	}, nil
}

func NewMem() (*MuxDB, error) {
	db, err := openMemDB()
	if err != nil {
		return nil, err
	}

	return &MuxDB{
		engine: db,
		cache:  nil,
	}, nil
}

func mk1(hash []byte, path []byte) []byte {
	var key [40]byte
	for i := 0; i < len(path) && i < 15; i++ {
		if i%2 == 0 {
			key[i/2] |= path[i] << 4
		} else {
			key[i/2] |= path[i]
		}
	}
	end := len(path)
	if end > 15 {
		end = 15
	}
	key[7] = key[7] | byte(end)
	copy(key[8:], hash)
	return key[:]
}

func mk2(hash []byte, path []byte) []byte {
	var key [40]byte

	l := len(path)
	if l > 15 {
		l = 15
	}

	if l > 4 {
		key[0] |= 1 << 4
	}

	for i := 0; i < l && i < 14; i++ {
		if i%2 == 0 {
			key[i/2] |= path[i]
		} else {
			key[i/2+1] |= path[i] << 4
		}
	}

	key[7] |= byte(l)

	copy(key[8:], hash)
	return key[:]
}
func mk3(hash []byte, path []byte) []byte {
	var key [40]byte

	l := len(path)
	if l > 15 {
		l = 15
	}

	if l > 3 {
		key[0] |= 1 << 4
	}

	for i := 0; i < l && i < 14; i++ {
		if i%2 == 0 {
			key[i/2] |= path[i]
		} else {
			key[i/2+1] |= path[i] << 4
		}
	}

	key[7] |= byte(l)

	copy(key[8:], hash)
	return key[:]
}

func mk4(hash []byte, path []byte) []byte {
	var key [36]byte

	for i := 0; i < len(path) && i < 7; i++ {
		if i%2 == 0 {
			key[i/2] |= (path[i] << 4)
		} else {
			key[i/2] |= path[i]
		}
	}

	l := len(path)
	if l > 15 {
		l = 15
	}

	key[3] |= byte(l)
	copy(key[4:], hash)
	return key[:]
}

func MakeHashKey(hash []byte, path []byte) []byte {
	return mk1(hash, path)
}

func (m *MuxDB) NewTrie(name string, root thor.Bytes32, secure bool) Trie {
	return m.newTrie(name, root, secure, false)
}

func (m *MuxDB) NewTrieNoFillCache(name string, root thor.Bytes32, secure bool) Trie {
	return m.newTrie(name, root, secure, true)
}

func (m *MuxDB) newTrie(name string, root thor.Bytes32, secure bool, dontFillCache bool) Trie {

	dontFillCache = true
	bkt := bucket{trieSlot} //append(bucket{trieSlot}, []byte(name)...)
	nbkt := bucket(name)
	m.lock.Lock()
	t := m.trix
	m.lock.Unlock()

	raw, err := trie.New(root, struct {
		getFunc
		hasFunc
		putFunc
		getExFunc
	}{
		nil,
		nil,
		nil,
		func(key, path []byte, decode func([]byte) interface{}) ([]byte, interface{}, error) {
			noFill := dontFillCache
			if len(path) < 4 {
				noFill = false
			}
			enc, err := m.cache.ProxyGet(func(key []byte) ([]byte, error) {
				var val []byte
				if err := m.engine.Snapshot(func(getter kv.Getter) error {
					v, err := nbkt.ProxyGet(t.ProxyGet(bkt.ProxyGet(getter.Get)))(MakeHashKey(key, path))
					if err != nil {
						return err
					}
					val = v
					return nil
				}); err != nil {
					return nil, err
				}
				return val, nil
			}, noFill)(key)

			if err != nil {
				return nil, nil, err
			}

			if dec := m.nodeCache.Get(key); dec != nil {
				return enc, dec, nil
			}

			dec := decode(enc)
			if !noFill {
				m.nodeCache.Set(key, dec)
			}

			return enc, dec, nil
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
			func(fn func(putExFunc) error) error {

				return m.engine.Batch(func(putter kv.Putter) error {
					saveSecureKey := bucket{trieSecureKeySlot}.ProxyPut(putter.Put)
					for h, p := range secureKeyPreimages {
						if err := saveSecureKey(h[:], p); err != nil {
							return err
						}
					}
					secureKeyPreimages = nil

					x := nbkt.ProxyPut(t.ProxyPut(bkt.ProxyPut(putter.Put)))
					return fn(func(key, path, val []byte, dec interface{}) error {

						m.nodeCache.Set(key, dec)
						return m.cache.ProxyPut(func(k, v []byte) error {
							return x(MakeHashKey(k, path), v)
						})(key, val)

					})
				})
			},
		}
	}

	return &trieWrap{
		raw,
		err,
		func(key []byte, save bool) []byte { return key },
		func(fn func(putExFunc) error) error {
			return m.engine.Batch(func(putter kv.Putter) error {
				x := nbkt.ProxyPut(t.ProxyPut(bkt.ProxyPut(putter.Put)))
				return fn(func(key, path, val []byte, dec interface{}) error {

					m.nodeCache.Set(key, dec)
					return m.cache.ProxyPut(func(k, v []byte) error {
						return x(MakeHashKey(k, path), v)
					})(key, val)

				})
			})
		},
	}
}
func (m *MuxDB) EvictTrieCache(key []byte) {
	m.cache.Evict(key)
}

func (m *MuxDB) RollTrie(i int) ([]byte, []byte, []byte) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.trix++

	p1, p2, p3 := m.trix.Prefixes()

	return []byte{trieSlot, p1},
		[]byte{trieSlot, p2},
		[]byte{trieSlot, p3}
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
		func(r kv.Range, fn func([]byte, []byte) bool) error {
			bucketLen := len(b)
			return m.engine.Iterate(kv.Range{
				From: append(b, r.From...),
				To:   append(b, r.To...),
			}, func(key, val []byte) bool {
				return fn(key[bucketLen:], val)
			})
		},
		m.engine.IsNotFound,
		func(r kv.Range) error { return errors.New("not supported") },
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
