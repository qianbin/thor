package muxdb

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

func (b bucket) ProxyGetter(getter Getter) Getter {
	return &struct {
		getFunc
		hasFunc
	}{
		b.ProxyGet(getter.Get),
		b.ProxyHas(getter.Has),
	}
}

func (b bucket) ProxyPutter(putter Putter) Putter {
	return &struct {
		putFunc
		deleteFunc
	}{
		b.ProxyPut(putter.Put),
		b.ProxyDelete(putter.Delete),
	}
}

func (b bucket) ProxyKV(kv KV) KV {
	return &struct {
		Getter
		Putter
		snapshotFunc
		batchFunc
		iterateFunc
		isNotFoundFunc
	}{
		b.ProxyGetter(kv),
		b.ProxyPutter(kv),
		func(fn func(Getter) error) error {
			return kv.Snapshot(func(getter Getter) error {
				return fn(b.ProxyGetter(getter))
			})
		},
		func(fn func(Putter) error) error {
			return kv.Batch(func(putter Putter) error {
				return fn(b.ProxyPutter(putter))
			})
		},
		func(prefix []byte, fn func([]byte, []byte) error) error {
			bucketLen := len(b)
			return kv.Iterate(append(b, prefix...), func(key, val []byte) error {
				return fn(key[bucketLen:], val)
			})
		},
		kv.IsNotFound,
	}
}

func (b bucket) makeKey(key []byte) []byte {
	return append(b, key...)
}
