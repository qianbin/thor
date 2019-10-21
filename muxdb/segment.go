package muxdb

import "math"

type segment uint16

func (s segment) ProxyGet(get getFunc) getFunc {
	return func(key []byte) ([]byte, error) {
		val, err := get(s.makeKey(key))
		if err == nil {
			return val, nil
		}
		if s == math.MaxUint16 {
			return nil, err
		}

		val, err = get(segment(math.MaxUint16).makeKey(key))
		if err == nil {
			return val, nil
		}

		for i := int(s) - 1; i >= 0; i-- {
			val, err = get(segment(i).makeKey(key))
			if err == nil {
				return val, nil
			}
		}
		return val, err
	}
}

func (s segment) ProxyPut(put putFunc) putFunc {
	return func(key, val []byte) error {
		return put(s.makeKey(key), val)
	}
}

func (s segment) makeKey(key []byte) []byte {
	return append([]byte{byte(s >> 8), byte(s)}, key...)
}
