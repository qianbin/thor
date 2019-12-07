package kv

// Getter defines methods to read kv.
type Getter interface {
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
}

// Putter defines methods to write kv.
type Putter interface {
	Put(key, val []byte) error
	Delete(key []byte) error
}

// PutCommitter defines putter with commit method.
type PutCommitter interface {
	Putter
	Commit() error
}

// Pair defines key-value pair.
type Pair interface {
	Key() []byte
	Value() []byte
}

// Range is the key range.
type Range struct {
	Start []byte // start of key range (included)
	Limit []byte // limit of key range (excluded)
}

// Store defines the full functional kv store.
type Store interface {
	Getter
	Putter

	Snapshot(fn func(Getter) error) error
	Batch(fn func(PutCommitter) error) error
	Iterate(r Range, fn func(Pair) bool) error
	IsNotFound(err error) bool
}
