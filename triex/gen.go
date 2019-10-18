package triex

import (
	"encoding/binary"
	"math"
)

type genr uint16

func (g genr) ProxyGetter(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		val, err := g.get(get, key)
		if err == nil {
			return val, nil
		}

		if g == math.MaxUint16 {
			return nil, err
		}

		val, err = genr(math.MaxUint16).get(get, key)
		if err == nil {
			return val, nil
		}

		i := uint16(g)
		for {
			if i == 0 {
				return nil, err
			}
			i--

			val, err = genr(i).get(get, key)
			if err == nil {
				return val, nil
			}
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

func (g genr) get(get getFunc, key []byte) ([]byte, error) {
	keyBuf := keyBufPool.Get().([]byte)
	val, err := get(g.makeKey(keyBuf, key, uint16(g)))
	keyBufPool.Put(keyBuf)
	return val, err
}

func (g genr) makeKey(buf []byte, key []byte, i uint16) []byte {
	k := append(append(buf, 0), 0)
	binary.BigEndian.PutUint16(k[:], i)
	return append(k, key...)
}
