package muxdb

import (
	"encoding/binary"
	"math/rand"

	"github.com/vechain/thor/thor"
)

type bloom struct {
	bits  []uint64
	m     int64
	k     int
	masks []uint64
}

func NewBloom(m int64, k int) *bloom {
	m = m / 64 * 64
	masks := make([]uint64, k)
	for i := 0; i < k; i++ {
		masks[i] = rand.Uint64()
	}
	return &bloom{
		make([]uint64, m/64),
		m,
		k,
		masks,
	}
}

func (b *bloom) Add(h thor.Bytes32) {
	b.distribute(h, func(index int, bit uint64) bool {
		b.bits[index] |= bit
		return true
	})
}

func (b *bloom) Test(h thor.Bytes32) bool {
	return b.distribute(h, func(index int, bit uint64) bool {
		return b.bits[index]&bit == bit
	})
}

func (b *bloom) distribute(h thor.Bytes32, cb func(index int, bit uint64) bool) bool {
	h64 := binary.BigEndian.Uint64(h[:])

	for i := 0; i < b.k; i++ {
		loc := (h64 ^ b.masks[i]) % uint64(b.m)
		bit := uint64(1) << (loc % 64)
		if !cb(int(loc/64), bit) {
			return false
		}
	}
	return true
}

func (b *bloom) Reset() {
	for i := 0; i < len(b.bits); i++ {
		b.bits[i] = 0
	}
}
