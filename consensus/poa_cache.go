package consensus

import (
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/vechain/thor/builtin/authority"
	"github.com/vechain/thor/poa"
	"github.com/vechain/thor/thor"
)

type poaCache struct {
	lru *simplelru.LRU
}

func newPOACache(size int) *poaCache {
	lru, _ := simplelru.NewLRU(size, nil)
	return &poaCache{lru}
}

func (c *poaCache) Get(blockID thor.Bytes32) *poaCacheEntry {
	if cached, ok := c.lru.Get(blockID); ok {
		return cached.(*poaCacheEntry)
	}
	return nil
}

func (c *poaCache) Set(blockID thor.Bytes32, entry *poaCacheEntry) {
	c.lru.Add(blockID, entry)
}

type poaCacheEntry struct {
	proposers []poa.Proposer
	m         map[thor.Address]int
}

func newPOACacheEntry(proposers []poa.Proposer, candidates []*authority.Candidate) *poaCacheEntry {
	m := make(map[thor.Address]int)
	for i, p := range proposers {
		m[p.Address] = i
	}
	for _, c := range candidates {
		if _, has := m[c.NodeMaster]; !has {
			m[c.NodeMaster] = -1
		}
		if _, has := m[c.Endorsor]; !has {
			m[c.Endorsor] = -1
		}
	}
	return &poaCacheEntry{
		proposers,
		m,
	}
}

func (e *poaCacheEntry) Contains(addr thor.Address) bool {
	_, has := e.m[addr]
	return has
}

func (e *poaCacheEntry) Update(addr thor.Address, active bool) {
	i, has := e.m[addr]
	if !has {
		panic("address should in map")
	}
	e.proposers[i].Active = active
}

func (e *poaCacheEntry) Copy() *poaCacheEntry {
	copied := make([]poa.Proposer, len(e.proposers))
	copy(copied, e.proposers)
	return &poaCacheEntry{
		copied,
		e.m,
	}
}
