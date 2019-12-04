package kv

// Engine describes the kv engine.
type Engine interface {
	Store
	Close() error
}
