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
	Iterate(prefix []byte, fn func(key, val []byte) error) error
	Compact(prefix []byte) error

	IsNotFound(error) bool
}
