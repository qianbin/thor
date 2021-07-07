package muxdb

import (
	"sync"

	"github.com/coocood/freecache"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/kv"
)

type mem map[string][]byte

type leafStatus struct {
	Ver      uint32
	Flushing bool
}

type TrieLeafCache struct {
	props kv.Store
	rMem  mem
	wMem  mem
	cache *freecache.Cache
	disk  kv.Store
	lock  sync.RWMutex
	ver   uint32
	dirty bool
}

const leafStatusKey = "leafStatus"

func newTrieLeafCache(props, leafDisk kv.Store) (*TrieLeafCache, error) {
	val, err := props.Get([]byte(leafStatusKey))
	if err != nil && !props.IsNotFound(err) {
		return nil, err
	}

	var status leafStatus
	if len(val) > 0 {
		if err := rlp.DecodeBytes(val, &status); err != nil {
			return nil, err
		}
	}

	return &TrieLeafCache{
		cache: freecache.NewCache(1024 * 1024 * 1024),
		disk:  leafDisk,
		ver:   status.Ver,
		dirty: status.Flushing,
		props: props,
	}, nil
}

func (c *TrieLeafCache) Ver() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.ver
}
func (c *TrieLeafCache) Put(key, value []byte) {
	if c.wMem == nil {
		c.wMem = make(mem)
	}
	if len(value) == 0 {
		c.wMem[string(key)] = nil
	} else {
		c.wMem[string(key)] = append([]byte(nil), value...)
	}
}

func (c *TrieLeafCache) Flush(ver uint32) error {
	c.lock.Lock()
	if c.wMem != nil {
		c.rMem = c.wMem
	} else {
		c.rMem = make(mem)
	}
	lastVer := c.ver
	c.ver = ver
	c.lock.Unlock()

	enc, _ := rlp.EncodeToBytes(&leafStatus{
		Ver:      lastVer,
		Flushing: true,
	})
	if err := c.props.Put([]byte(leafStatusKey), enc); err != nil {
		return err
	}

	err := c.disk.Batch(func(putter kv.Putter) error {
		n := 0
		for k, v := range c.wMem {
			k := []byte(k)
			if len(v) > 0 {
				if err := putter.Put(k, v); err != nil {
					return err
				}
			} else {
				if err := putter.Delete(k); err != nil {
					return err
				}
			}
			c.cache.Set(k, v, 8*3600)
			n++
		}
		return nil
	})
	c.wMem = nil
	if err != nil {
		return err
	}

	enc, _ = rlp.EncodeToBytes(&leafStatus{
		Ver:      ver,
		Flushing: false,
	})
	return c.props.Put([]byte(leafStatusKey), enc)
}

func (c *TrieLeafCache) Get(rootv, ver uint32, key []byte) ([]byte, []byte, bool, error) {
	enc, ok, err := c.get(rootv, ver, key)
	if err != nil {
		return nil, nil, false, err
	}
	if len(enc) == 0 {
		return nil, nil, ok, nil
	}

	var x struct {
		Val, Extra []byte
	}
	if err := rlp.DecodeBytes(enc, &x); err != nil {
		return nil, nil, false, err
	}
	return x.Val, x.Extra, ok, nil
}

func (c *TrieLeafCache) get(rootv uint32, ver uint32, key []byte) ([]byte, bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.ver == 0 || ver > c.ver || c.dirty || rootv < c.ver {
		return nil, false, nil
	}

	if val, ok := c.rMem[string(key)]; ok {
		if len(val) == 0 {
			return nil, true, nil
		}
		return append([]byte(nil), val...), true, nil
	}
	val, err := c.cache.Get(key)
	if err == nil {
		return val, true, nil
	}

	if err != freecache.ErrNotFound {
		return nil, false, err
	}

	val, err = c.disk.Get(key)
	if err != nil {
		if c.disk.IsNotFound(err) {
			c.cache.Set(key, nil, 8*3600)
			return nil, true, nil
		}
		return nil, false, err
	}
	c.cache.Set(key, val, 8*3600)
	return val, true, nil
}
