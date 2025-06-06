package coordinator

import (
	"context"
	"fmt"
	"log"

	"github.com/harshitster/223B-Project/code/proto"
)

type TxnStatus string

const (
	TxnPending   TxnStatus = "PENDING"
	TxnPrepared  TxnStatus = "PREPARED"
	TxnCommitted TxnStatus = "COMMITTED"
	TxnAborted   TxnStatus = "ABORTED"
)

type Transaction struct {
	ID         string    `json:"id"`
	From       string    `json:"from"`
	To         string    `json:"to"`
	Amount     int       `json:"amount"`
	Status     TxnStatus `json:"status"`
	Timestamp  int64     `json:"timestamp"`
	ClientAddr string    `json:"client_addr"`
}

func (c *Coordinator) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
	log.Printf("Txn: Received transaction request - ID: %s, Op: %s, Amount: %d, ClientAddr: %s",
		req.Id, req.Op, req.Amount, req.ClientAddr)

	c.TxnMu.Lock()

	if existing, ok := c.TxnMap[req.Id]; ok {
		if existing.Status == TxnCommitted {
			c.TxnMu.Unlock()
			log.Printf("Txn: Transaction %s is already committed, rejecting duplicate", req.Id)
			return &proto.TxnResponse{Accepted: false}, nil
		}
		c.TxnMu.Unlock()
		log.Printf("Txn: Transaction %s is a duplicate in-flight transaction, rejecting", req.Id)
		return &proto.TxnResponse{Accepted: false}, nil
	}
	c.TxnMu.Unlock()

	txn := &Transaction{
		ID:         req.Id,
		Amount:     int(req.Amount),
		ClientAddr: req.ClientAddr,
	}

	log.Printf("Txn: Processing operation type: %s for transaction %s", req.Op, req.Id)
	switch req.Op {
	case "transfer":
		if len(req.Accounts) < 2 {
			log.Printf("Txn: Transfer operation failed - insufficient accounts provided for transaction %s (got %d, need 2)",
				req.Id, len(req.Accounts))
			return &proto.TxnResponse{Accepted: false}, fmt.Errorf("transfer requires two accounts")
		}
		txn.From = req.Accounts[0]
		txn.To = req.Accounts[1]
		log.Printf("Txn: Transfer setup - from: %s, to: %s, amount: %d for transaction %s",
			txn.From, txn.To, txn.Amount, req.Id)

	case "credit":
		if len(req.Accounts) < 1 {
			log.Printf("Txn: Credit operation failed - no account provided for transaction %s", req.Id)
			return &proto.TxnResponse{Accepted: false}, fmt.Errorf("credit requires one account")
		}
		txn.To = req.Accounts[0]
		log.Printf("Txn: Credit setup - to: %s, amount: %d for transaction %s",
			txn.To, txn.Amount, req.Id)

	case "debit":
		if len(req.Accounts) < 1 {
			log.Printf("Txn: Debit operation failed - no account provided for transaction %s", req.Id)
			return &proto.TxnResponse{Accepted: false}, fmt.Errorf("debit requires one account")
		}
		txn.From = req.Accounts[0]
		log.Printf("Txn: Debit setup - from: %s, amount: %d for transaction %s",
			txn.From, txn.Amount, req.Id)

	default:
		log.Printf("Txn: Unsupported operation %s for transaction %s", req.Op, req.Id)
		return &proto.TxnResponse{Accepted: false}, fmt.Errorf("unsupported operation: %s", req.Op)
	}

	log.Printf("Txn: Attempting to queue transaction %s", req.Id)
	select {
	case c.ProcessChannel <- txn:
		log.Printf("Txn: Successfully queued transaction %s for processing", req.Id)
		return &proto.TxnResponse{Accepted: true}, nil
	case <-ctx.Done():
		log.Printf("Txn: Context cancelled or timed out while queuing transaction %s", req.Id)
		return &proto.TxnResponse{Accepted: false}, fmt.Errorf("context cancelled or timed out")
	}
}

func (c *Coordinator) AckTxn(ctx context.Context, req *proto.AckTxnRequest) (*proto.CoordAck, error) {
	log.Printf("AckTxn: Received acknowledgment request for transaction %s", req.TxnId)

	c.TxnMu.Lock()
	defer c.TxnMu.Unlock()
	log.Printf("AckTxn: Acquired read lock on transaction mutex for ID: %s", req.TxnId)

	txnId := req.TxnId
	txn, ok := c.TxnMap[txnId]
	if !ok {
		log.Printf("AckTxn: Transaction %s not found in transaction map", txnId)
		return &proto.CoordAck{Success: false}, nil
	}

	if txn.Status != TxnCommitted {
		log.Printf("AckTxn: Transaction %s is not in committed state (current status: %v)", txnId, txn.Status)
		return &proto.CoordAck{Success: false}, nil
	}

	log.Printf("AckTxn: Acknowledgment received for committed transaction %s", txnId)
	return &proto.CoordAck{Success: true}, nil
}

func (c *Coordinator) GetStatus(ctx context.Context, req *proto.GetStatusRequest) (*proto.GetStatusReply, error) {
	log.Printf("GetStatus: Received status request for transaction %s", req.TxnId)

	c.TxnMu.Lock()
	defer c.TxnMu.Unlock()
	log.Printf("GetStatus: Acquired transaction mutex for transaction %s", req.TxnId)

	txn, ok := c.TxnMap[req.TxnId]
	if ok {
		log.Printf("GetStatus: Found transaction %s in map with status: %s", req.TxnId, txn.Status)
		return &proto.GetStatusReply{Status: string(txn.Status)}, nil
	}

	log.Printf("GetStatus: Transaction %s not found in transaction map, checking log file", req.TxnId)

	if c.hasTxnCommittedInLog(req.TxnId) {
		log.Printf("GetStatus: Transaction %s found as committed in log file", req.TxnId)
		return &proto.GetStatusReply{Status: "COMMITTED"}, nil
	}

	log.Printf("GetStatus: Transaction %s not found anywhere, returning UNKNOWN status", req.TxnId)
	return &proto.GetStatusReply{Status: "UNKNOWN"}, nil
}
