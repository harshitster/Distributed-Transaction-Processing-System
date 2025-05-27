package coordinator

import (
	"context"
	"fmt"

	"github.com/harshitster/223B-Project/src/coordinator/proto"
)

type RpcServer struct {
	proto.UnimplementedCoordinatorServiceServer
	Coord *Coordinator
}

func (r *RpcServer) Submit(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
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

	return &proto.TxnResponse{Accepted: true}, nil
}
