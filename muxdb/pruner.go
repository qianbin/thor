package muxdb

import (
	"encoding/binary"

	"github.com/inconshreveable/log15"
)

var (
	log              = log15.New("pkg", "muxdb")
	prunedSegmentKey = []byte("_pruned-segment")
)

func (m *MuxDB) prune(newBlockCh <-chan *uint32) error {
	// var (
	// 	bloom     = newBloom(64*1024*1024*8, 3) // 64 MB
	// 	bloomLock sync.Mutex
	// 	goes      co.Goes
	// 	seg       = m.loadPrunedSegment()
	// )

	for {

	}
	return nil
}

func (m *MuxDB) loadPrunedSegment() uint16 {
	config := m.NewBucket("config")
	val, err := config.Get(prunedSegmentKey)
	if err != nil {
		if !config.IsNotFound(err) {
			log.Warn("load pruned segment", "err", err)
		}
		return 0
	}
	if len(val) != 2 {
		log.Warn("load pruned segment, malformed value")
		return 0
	}
	return binary.BigEndian.Uint16(val)
}

func (m *MuxDB) savePrunedSegment(seg uint16) {
	config := m.NewBucket("config")
	var val [2]byte
	binary.BigEndian.PutUint16(val[:], seg)
	if err := config.Put(prunedSegmentKey, val[:]); err != nil {
		log.Warn("save pruned segment", "err", err)
	}
}
