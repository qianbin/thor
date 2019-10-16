package triex

import (
	"encoding/binary"
	"math"
)

type genr uint16

func (g genr) ProxyGetter(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		i := uint16(g)
		for {
			keyBuf := keyBufPool.Get().([]byte)
			val, err := get(g.makeKey(keyBuf, key, i))
			keyBufPool.Put(keyBuf)
			if i == (math.MaxUint32 / 100000) {
				return val, err
			}
			if err == nil {
				return val, nil
			}
			i--
		}
	}
}

func (g genr) ProxyPutter(put putFunc) putFunc {
	return func(key, val []byte) error {
		keyBuf := keyBufPool.Get().([]byte)
		err := put(g.makeKey(keyBuf, key, uint16(g)), val)
		keyBufPool.Put(keyBuf)
		return err
	}
}

func (g genr) makeKey(buf []byte, key []byte, i uint16) []byte {
	k := append(append(buf, 0), 0)
	binary.BigEndian.PutUint16(k[:], i)
	return append(k, key...)
}
