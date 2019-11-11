package muxdb

import "github.com/vechain/thor/kv"

type engine interface {
	kv.Store
	Close() error
}

type getFunc func(key []byte) ([]byte, error)
type hasFunc func(key []byte) (bool, error)
type putFunc func(key, val []byte) error
type deleteFunc func(key []byte) error
type isNotFoundFunc func(error) bool
type snapshotFunc func(func(getter kv.Getter) error) error
type batchFunc func(func(putter kv.Putter) error) error
type iterateFunc func(r kv.Range, fn func(key, val []byte) bool) error
type compactFunc func(r kv.Range) error
type getExFunc func(key, path []byte, decode func(enc []byte) interface{}) ([]byte, interface{}, error)
type putExFunc func(key, path, value []byte, dec interface{}) error

func (f getFunc) Get(key []byte) ([]byte, error)               { return f(key) }
func (f hasFunc) Has(key []byte) (bool, error)                 { return f(key) }
func (f putFunc) Put(key, val []byte) error                    { return f(key, val) }
func (f deleteFunc) Delete(key []byte) error                   { return f(key) }
func (f isNotFoundFunc) IsNotFound(err error) bool             { return f(err) }
func (f snapshotFunc) Snapshot(fn func(kv.Getter) error) error { return f(fn) }
func (f batchFunc) Batch(fn func(kv.Putter) error) error       { return f(fn) }
func (f iterateFunc) Iterate(r kv.Range, fn func(key, val []byte) bool) error {
	return f(r, fn)
}
func (f compactFunc) Compact(r kv.Range) error { return f(r) }
func (f getExFunc) GetEx(key, path []byte, decode func([]byte) interface{}) ([]byte, interface{}, error) {
	return f(key, path, decode)
}
func (f putExFunc) PutEx(key, path, val []byte, dec interface{}) error {
	return f(key, path, val, dec)
}
