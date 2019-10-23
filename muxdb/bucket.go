package muxdb

import "github.com/vechain/thor/kv"

type bucket []byte

func (b bucket) ProxyGet(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		return get(b.makeKey(key))
	}
}

func (b bucket) ProxyHas(has hasFunc) hasFunc {
	return func(key []byte) (bool, error) {
		return has(b.makeKey(key))
	}
}

func (b bucket) ProxyPut(put putFunc) putFunc {
	return func(key, val []byte) error {
		return put(b.makeKey(key), val)
	}
}

func (b bucket) ProxyDelete(del deleteFunc) deleteFunc {
	return func(key []byte) error {
		return del(b.makeKey(key))
	}
}

func (b bucket) ProxyGetter(getter kv.Getter) kv.Getter {
	return &struct {
		getFunc
		hasFunc
	}{
		b.ProxyGet(getter.Get),
		b.ProxyHas(getter.Has),
	}
}

func (b bucket) ProxyPutter(putter kv.Putter) kv.Putter {
	return &struct {
		putFunc
		deleteFunc
	}{
		b.ProxyPut(putter.Put),
		b.ProxyDelete(putter.Delete),
	}
}

func (b bucket) makeKey(key []byte) []byte {
	return append(b, key...)
}
