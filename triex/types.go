// Copyright (c) 2019 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package triex

type getFunc func(key []byte) ([]byte, error)
type hasFunc func(key []byte) (bool, error)
type putFunc func(key, val []byte) error

func (f getFunc) Get(key []byte) ([]byte, error) { return f(key) }
func (f hasFunc) Has(key []byte) (bool, error)   { return f(key) }
func (f putFunc) Put(key, val []byte) error      { return f(key, val) }
