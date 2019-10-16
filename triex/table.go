// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package triex

import "sync"

var keyBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 33)
	},
}

type table byte

func (t table) ProxyGetter(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		keyBuf := keyBufPool.Get().([]byte)
		val, err := get(t.makeKey(keyBuf, key))
		keyBufPool.Put(keyBuf)
		return val, err
	}
}

func (t table) ProxyPutter(put putFunc) putFunc {
	return func(key, val []byte) error {
		keyBuf := keyBufPool.Get().([]byte)
		err := put(t.makeKey(keyBuf, key), val)
		keyBufPool.Put(keyBuf)
		return err
	}
}

func (t table) makeKey(buf []byte, key []byte) []byte {
	return append(append(buf, byte(t)), key...)
}

// type dualTable [2]table

// func (d *dualTable) ProxyGetter(get getFunc) getFunc {
// 	g0 := d[0].ProxyGetter(get)
// 	g1 := d[1].ProxyGetter(get)

// 	return func(key []byte) ([]byte, error) {
// 		val, err := g1.Get(key)
// 		if err == nil {
// 			return val, nil
// 		}
// 		return g0.Get(key)
// 	}
// }

// func (d *dualTable) ProxyPutter(put putFunc) putFunc {
// 	p0 := d[0].ProxyPutter(put)
// 	p1 := d[1].ProxyPutter(put)

// 	return func(key, val []byte) error {
// 		if err := p1.Put(key, val); err != nil {
// 			return err
// 		}
// 		return p0.Put(key, val)
// 	}
// }
