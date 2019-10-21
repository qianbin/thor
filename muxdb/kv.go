package muxdb

type Getter interface {
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
}

type Putter interface {
	Put(key, value []byte) error
	Delete(key []byte) error
}

type KV interface {
	Getter
	Putter
	Snapshot(fn func(snapshot Getter) error) error
	Batch(fn func(batch Putter) error) error
	Iterate(prefix []byte, fn func(key, val []byte) error) error

	IsNotFound(error) bool
}

type engine interface {
	KV
	Close() error
	Compact(prefix []byte) error
}

type getFunc func(key []byte) ([]byte, error)
type hasFunc func(key []byte) (bool, error)
type putFunc func(key, val []byte) error
type deleteFunc func(key []byte) error
type isNotFoundFunc func(error) bool
type snapshotFunc func(func(getter Getter) error) error
type batchFunc func(func(putter Putter) error) error
type iterateFunc func(prefix []byte, fn func(key, val []byte) error) error

func (f getFunc) Get(key []byte) ([]byte, error)            { return f(key) }
func (f hasFunc) Has(key []byte) (bool, error)              { return f(key) }
func (f putFunc) Put(key, val []byte) error                 { return f(key, val) }
func (f deleteFunc) Delete(key []byte) error                { return f(key) }
func (f isNotFoundFunc) IsNotFound(err error) bool          { return f(err) }
func (f snapshotFunc) Snapshot(fn func(Getter) error) error { return f(fn) }
func (f batchFunc) Batch(fn func(Putter) error) error       { return f(fn) }
func (f iterateFunc) Iterate(prefix []byte, fn func(key, val []byte) error) error {
	return f(prefix, fn)
}
