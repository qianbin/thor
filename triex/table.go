// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package triex

type table string

func (t table) ProxyGetter(get getFunc) getFunc {
	var keyBuf []byte
	return func(key []byte) ([]byte, error) {
		return get(t.makeKey(&keyBuf, key))
	}
}

func (t table) ProxyPutter(put putFunc) putFunc {
	var keyBuf []byte
	return func(key, val []byte) error {
		return put(t.makeKey(&keyBuf, key), val)
	}
}

func (t table) makeKey(buf *[]byte, key []byte) []byte {
	*buf = (*buf)[:0]
	*buf = append(append(*buf, []byte(t)...), key...)
	return *buf
}

type dualTable [2]table

func (d *dualTable) ProxyGetter(get getFunc) getFunc {
	g0 := d[0].ProxyGetter(get)
	g1 := d[1].ProxyGetter(get)

	return func(key []byte) ([]byte, error) {
		val, err := g0.Get(key)
		if err == nil {
			return val, nil
		}
		return g1.Get(key)
	}
}

func (d *dualTable) ProxyPutter(put putFunc) putFunc {
	p0 := d[0].ProxyPutter(put)
	p1 := d[1].ProxyPutter(put)

	return func(key, val []byte) error {
		if err := p0.Put(key, val); err != nil {
			return err
		}
		return p1.Put(key, val)
	}
}
