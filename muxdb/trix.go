package muxdb

type trix int

func (t trix) ProxyGet(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		key = append([]byte{0}, key...)
		val, err := get(key)
		if err == nil {
			return val, nil
		}

		key[0] = byte(t%2 + 1)
		val, err = get(key)
		if err == nil {
			return val, nil
		}
		key[0] = byte((t+1)%2 + 1)
		return get(key)
	}
}

func (t trix) ProxyPut(put putFunc) putFunc {
	return func(key, val []byte) error {
		return put(append([]byte{byte(t%2 + 1)}, key...), val)
	}
}

func (t trix) Prefixes() (byte, byte, byte) {
	return byte(t%2 + 1), byte((t+1)%2 + 1), 0
}
