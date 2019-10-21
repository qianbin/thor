package muxdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vechain/thor/thor"
)

func TestBloom(t *testing.T) {
	b := newBloom(1024*1024, 3)

	h1 := thor.Blake2b([]byte("hello"))
	assert.False(t, b.Test(h1))
	b.Add(h1)
	assert.True(t, b.Test(h1))
}
