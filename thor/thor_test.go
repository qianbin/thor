// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package thor

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stretchr/testify/assert"
)

func TestBytes32(t *testing.T) {
	var a = [][]byte{{1, 2}, {3, 4}}
	data, _ := rlp.EncodeToBytes(a)
	fmt.Println(data)
	c, r, _ := rlp.SplitList(data)
	fmt.Println(r)
	for {
		_, c, r, _ = rlp.Split(c)
		fmt.Println(c)
		if len(r) > 0 {
			c = r
		} else {
			break
		}
	}

}

func TestAddress(t *testing.T) {
	addr := BytesToAddress([]byte("addr"))
	data, _ := json.Marshal(&addr)
	assert.Equal(t, "\""+addr.String()+"\"", string(data))

	var dec Address
	assert.Nil(t, json.Unmarshal(data, &dec))
	assert.Equal(t, addr, dec)
}
