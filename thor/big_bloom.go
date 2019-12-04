package thor

import "encoding/binary"

type BigBloom struct {
	bits []byte
	k    int
}

func NewBigBloom(sizeMB int, k int) *BigBloom {
	if k > 8 {
		k = 8
	}
	if k < 1 {
		k = 1
	}
	return &BigBloom{
		make([]byte, sizeMB*1024*1024),
		k,
	}
}

func (bb *BigBloom) Add(h Bytes32) {
	bb.distribute(h, func(index int, bit byte) bool {
		bb.bits[index] |= bit
		return true
	})
}

func (bb *BigBloom) Test(h Bytes32) bool {
	return bb.distribute(h, func(index int, bit byte) bool {
		return bb.bits[index]&bit == bit
	})
}

func (bb *BigBloom) distribute(h Bytes32, cb func(index int, big byte) bool) bool {
	for i := 0; i < bb.k; i++ {
		d := binary.BigEndian.Uint32(h[i*4:]) % uint32(len(bb.bits)*8)
		bit := byte(1) << (d % 8)
		if !cb(int(d/8), bit) {
			return false
		}
	}
	return true
}

func (bb *BigBloom) Reset() {
	for i := 0; i < len(bb.bits); i++ {
		bb.bits[i] = 0
	}
}
