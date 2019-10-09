// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package transactions

import (
	"net/http"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/vechain/thor/api/utils"
	"github.com/vechain/thor/chain"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/txpool"
)

type Transactions struct {
	chain *chain.Chain
	pool  *txpool.TxPool
}

func New(chain *chain.Chain, pool *txpool.TxPool) *Transactions {
	return &Transactions{
		chain,
		pool,
	}
}

func (t *Transactions) getRawTransaction(txID thor.Bytes32, blockID thor.Bytes32) (*rawTransaction, error) {
	tx, txMeta, err := t.chain.NewBranch(blockID).GetTransaction(txID)
	if err != nil {
		if t.chain.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	header, err := t.chain.GetBlockHeader(txMeta.BlockID)
	if err != nil {
		return nil, err
	}
	raw, err := rlp.EncodeToBytes(tx)
	if err != nil {
		return nil, err
	}
	return &rawTransaction{
		RawTx: RawTx{hexutil.Encode(raw)},
		Meta: TxMeta{
			BlockID:        header.ID(),
			BlockNumber:    header.Number(),
			BlockTimestamp: header.Timestamp(),
		},
	}, nil
}

func (t *Transactions) getTransactionByID(txID thor.Bytes32, blockID thor.Bytes32) (*Transaction, error) {
	tx, txMeta, err := t.chain.NewBranch(blockID).GetTransaction(txID)
	if err != nil {
		if t.chain.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	header, err := t.chain.GetBlockHeader(txMeta.BlockID)
	if err != nil {
		return nil, err
	}
	return convertTransaction(tx, header, txMeta.Index)
}

//GetTransactionReceiptByID get tx's receipt
func (t *Transactions) getTransactionReceiptByID(txID thor.Bytes32, blockID thor.Bytes32) (*Receipt, error) {
	branch := t.chain.NewBranch(blockID)
	tx, txMeta, err := branch.GetTransaction(txID)
	if err != nil {
		if t.chain.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	receipt, err := branch.GetReceipt(txID)
	if err != nil {
		return nil, err
	}

	header, err := t.chain.GetBlockHeader(txMeta.BlockID)
	if err != nil {
		return nil, err
	}

	return convertReceipt(receipt, header, tx)
}
func (t *Transactions) handleSendTransaction(w http.ResponseWriter, req *http.Request) error {
	var rawTx *RawTx
	if err := utils.ParseJSON(req.Body, &rawTx); err != nil {
		return utils.BadRequest(errors.WithMessage(err, "body"))
	}
	tx, err := rawTx.decode()
	if err != nil {
		return utils.BadRequest(errors.WithMessage(err, "raw"))
	}

	if err := t.pool.Add(tx); err != nil {
		if txpool.IsBadTx(err) {
			return utils.BadRequest(err)
		}
		if txpool.IsTxRejected(err) {
			return utils.Forbidden(err)
		}
		return err
	}
	return utils.WriteJSON(w, map[string]string{
		"id": tx.ID().String(),
	})
}

func (t *Transactions) handleGetTransactionByID(w http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["id"]
	txID, err := thor.ParseBytes32(id)
	if err != nil {
		return utils.BadRequest(errors.WithMessage(err, "id"))
	}
	head, err := t.parseHead(req.URL.Query().Get("head"))
	if err != nil {
		return utils.BadRequest(errors.WithMessage(err, "head"))
	}
	h, err := t.chain.GetBlockHeader(head)
	if err != nil {
		if t.chain.IsNotFound(err) {
			return utils.BadRequest(errors.WithMessage(err, "head"))
		}
		return err
	}
	raw := req.URL.Query().Get("raw")
	if raw != "" && raw != "false" && raw != "true" {
		return utils.BadRequest(errors.WithMessage(errors.New("should be boolean"), "raw"))
	}
	if raw == "true" {
		tx, err := t.getRawTransaction(txID, h.ID())
		if err != nil {
			return err
		}
		return utils.WriteJSON(w, tx)
	}
	tx, err := t.getTransactionByID(txID, h.ID())
	if err != nil {
		return err
	}
	return utils.WriteJSON(w, tx)

}

func (t *Transactions) handleGetTransactionReceiptByID(w http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["id"]
	txID, err := thor.ParseBytes32(id)
	if err != nil {
		return utils.BadRequest(errors.WithMessage(err, "id"))
	}
	head, err := t.parseHead(req.URL.Query().Get("head"))
	if err != nil {
		return utils.BadRequest(errors.WithMessage(err, "head"))
	}
	h, err := t.chain.GetBlockHeader(head)
	if err != nil {
		if t.chain.IsNotFound(err) {
			return utils.BadRequest(errors.WithMessage(err, "head"))
		}
		return err
	}
	receipt, err := t.getTransactionReceiptByID(txID, h.ID())
	if err != nil {
		return err
	}
	return utils.WriteJSON(w, receipt)
}

func (t *Transactions) parseHead(head string) (thor.Bytes32, error) {
	if head == "" {
		return t.chain.BestBlock().Header().ID(), nil
	}
	h, err := thor.ParseBytes32(head)
	if err != nil {
		return thor.Bytes32{}, err
	}
	return h, nil
}

func (t *Transactions) Mount(root *mux.Router, pathPrefix string) {
	sub := root.PathPrefix(pathPrefix).Subrouter()

	sub.Path("").Methods("POST").HandlerFunc(utils.WrapHandlerFunc(t.handleSendTransaction))
	sub.Path("/{id}").Methods("GET").HandlerFunc(utils.WrapHandlerFunc(t.handleGetTransactionByID))
	sub.Path("/{id}/receipt").Methods("GET").HandlerFunc(utils.WrapHandlerFunc(t.handleGetTransactionReceiptByID))
}
