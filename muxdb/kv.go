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
type iterateFunc func(prefix []byte, fn func(key, val []byte) error) error
type compactFunc func(from, to []byte) error

func (f getFunc) Get(key []byte) ([]byte, error)               { return f(key) }
func (f hasFunc) Has(key []byte) (bool, error)                 { return f(key) }
func (f putFunc) Put(key, val []byte) error                    { return f(key, val) }
func (f deleteFunc) Delete(key []byte) error                   { return f(key) }
func (f isNotFoundFunc) IsNotFound(err error) bool             { return f(err) }
func (f snapshotFunc) Snapshot(fn func(kv.Getter) error) error { return f(fn) }
func (f batchFunc) Batch(fn func(kv.Putter) error) error       { return f(fn) }
func (f iterateFunc) Iterate(prefix []byte, fn func(key, val []byte) error) error {
	return f(prefix, fn)
}
func (f compactFunc) Compact(from, to []byte) error { return f(from, to) }
