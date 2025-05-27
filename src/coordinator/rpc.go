package coordinator

import (
	"context"
	"fmt"
)

type RpcServer struct {
	Coord *Coordinator
}

func (r *RpcServer) Submit(ctx context.Context, req *TxnRequest) (*TxnResponse, error) {
	txn := &Transaction{
		ID:       req.Id,
		Op:       req.Op,
		Accounts: req.Accounts,
		Amount:   int(req.Amount),
		Status:   TxnPending,
	}

	err := r.Coord.SubmitTransaction(txn)
	if err != nil {
		return nil, fmt.Errorf("failed to queue txn: %w", err)
	}

	return &TxnResponse{Accepted: true}, nil
}
