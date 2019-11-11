package kv

type Getter interface {
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
}

type Putter interface {
	Put(key, value []byte) error
	Delete(key []byte) error
}

type Store interface {
	Getter
	Putter
	Snapshot(fn func(Getter) error) error
	Batch(fn func(Putter) error) error
	Iterate(r Range, fn func(key, val []byte) bool) error
	Compact(r Range) error

	IsNotFound(error) bool
}

type Range struct {
	From, To []byte
}
