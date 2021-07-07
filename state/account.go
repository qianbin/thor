// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package state

import (
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/vechain/thor/muxdb"
	"github.com/vechain/thor/thor"
)

// Account is the Thor consensus representation of an account.
// RLP encoded objects are stored in main account trie.
type Account struct {
	Balance     *big.Int
	Energy      *big.Int
	BlockTime   uint64
	Master      []byte // master address
	CodeHash    []byte // hash of code
	StorageRoot []byte // merkle root of the storage trie
	meta        AccountMeta
}

type AccountMeta struct {
	Addr             thor.Address
	StorageCommitNum uint32
}

// IsEmpty returns if an account is empty.
// An empty account has zero balance and zero length code hash.
func (a *Account) IsEmpty() bool {
	return a.Balance.Sign() == 0 &&
		a.Energy.Sign() == 0 &&
		len(a.Master) == 0 &&
		len(a.CodeHash) == 0
}

var bigE18 = big.NewInt(1e18)

// CalcEnergy calculates energy based on current block time.
func (a *Account) CalcEnergy(blockTime uint64) *big.Int {
	if a.BlockTime == 0 {
		return a.Energy
	}

	if a.Balance.Sign() == 0 {
		return a.Energy
	}

	if blockTime <= a.BlockTime {
		return a.Energy
	}

	x := new(big.Int).SetUint64(blockTime - a.BlockTime)
	x.Mul(x, a.Balance)
	x.Mul(x, thor.EnergyGrowthRate)
	x.Div(x, bigE18)
	return new(big.Int).Add(a.Energy, x)
}

func emptyAccount() *Account {
	return &Account{Balance: &big.Int{}, Energy: &big.Int{}}
}

// loadAccount load an account object by address in trie.
// It returns empty account is no account found at the address.
func loadAccount(trie *muxdb.Trie, addr thor.Address) (*Account, error) {
	data, meta, err := trie.GetMeta(thor.Blake2b(addr[:]).Bytes())
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return emptyAccount(), nil
	}
	var a Account
	if err := rlp.DecodeBytes(data, &a); err != nil {
		return nil, err
	}
	if err := rlp.DecodeBytes(meta, &a.meta); err != nil {
		return nil, err
	}
	return &a, nil
}

// saveAccount save account into trie at given address.
// If the given account is empty, the value for given address is deleted.
func saveAccount(trie *muxdb.Trie, addr thor.Address, a *Account) error {
	if a.IsEmpty() {
		// delete if account is empty
		return trie.Update(thor.Blake2b(addr[:]).Bytes(), nil)
	}

	data, err := rlp.EncodeToBytes(a)
	if err != nil {
		return err
	}

	a.meta.Addr = addr
	meta, err := rlp.EncodeToBytes(&a.meta)
	if err != nil {
		return err
	}

	return trie.UpdateMeta(thor.Blake2b(addr[:]).Bytes(), data, meta)
}

// loadStorage load storage data for given key.
func loadStorage(trie *muxdb.Trie, key thor.Bytes32) (rlp.RawValue, error) {
	return trie.Get(thor.Blake2b(key[:]).Bytes())
}

// saveStorage save value for given key.
// If the data is zero, the given key will be deleted.
func saveStorage(trie *muxdb.Trie, key thor.Bytes32, data rlp.RawValue) error {
	return trie.UpdateMeta(thor.Blake2b(key[:]).Bytes(), data, key[:])
}
