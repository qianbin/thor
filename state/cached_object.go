// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package state

import (
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/triex"
)

// cachedObject to cache code and storage of an account.
type cachedObject struct {
	triex    *triex.Proxy
	data     Account
	blockNum uint32

	cache struct {
		code        []byte
		storageTrie triex.Trie
		storage     map[thor.Bytes32]rlp.RawValue
	}
}

func newCachedObject(triex *triex.Proxy, data *Account, blockNum uint32) *cachedObject {
	return &cachedObject{triex: triex, data: *data, blockNum: blockNum}
}

func (co *cachedObject) getOrCreateStorageTrie() triex.Trie {
	if co.cache.storageTrie == nil {
		co.cache.storageTrie = co.triex.NewTrie(
			thor.BytesToBytes32(co.data.StorageRoot), co.blockNum,
			true)
	}
	return co.cache.storageTrie
}

// GetStorage returns storage value for given key.
func (co *cachedObject) GetStorage(key thor.Bytes32) (rlp.RawValue, error) {
	cache := &co.cache
	// retrive from storage cache
	if cache.storage == nil {
		cache.storage = make(map[thor.Bytes32]rlp.RawValue)
	} else {
		if v, ok := cache.storage[key]; ok {
			return v, nil
		}
	}
	// not found in cache

	trie := co.getOrCreateStorageTrie()

	// load from trie
	v, err := trie.Get(key[:])
	if err != nil {
		return nil, err
	}
	// put into cache
	cache.storage[key] = v
	return v, nil
}

// GetCode returns the code of the account.
func (co *cachedObject) GetCode() ([]byte, error) {
	cache := &co.cache

	if len(cache.code) > 0 {
		return cache.code, nil
	}

	if len(co.data.CodeHash) > 0 {
		// do have code

		code, err := co.triex.GetPreimage(co.data.CodeHash)
		if err != nil {
			return nil, err
		}
		cache.code = code
		return code, nil
	}
	return nil, nil
}
