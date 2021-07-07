package muxdb

type trieSnapshotStatus struct {
	Ver uint32 //
}

type trieSnapshot struct {
}

func newTrieSnapshot() {

}

// Build build the mem-snap and commit into disk-snap.
func (s *trieSnapshot) NewMemSnap(toVer uint32) *trieMemSnap {
	return nil
}

func (s *trieSnapshot) Get(key []byte) ([]byte, error) {
	return nil, nil
}

type trieMemSnap struct {
}

func (m *trieMemSnap) Put(key, val []byte) {
}

func (m *trieMemSnap) Commit() error {
	return nil
}
