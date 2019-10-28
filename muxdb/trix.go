package muxdb

type trix [3]byte

func (t trix) ProxyGet(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		key = append([]byte{t[0]}, key...)
		val, err := get(key)
		if err == nil {
			return val, nil
		}

		key[0] = t[1]
		val, err = get(key)
		if err == nil {
			return val, nil
		}
		key[0] = t[2]
		return get(key)
	}
}

func (t trix) ProxyPut(put putFunc) putFunc {
	return func(key, val []byte) error {
		return put(append([]byte{t[0]}, key...), val)
	}
}
