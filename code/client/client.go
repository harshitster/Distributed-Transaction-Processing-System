package client

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	proto.UnimplementedClientServiceServer

	coordinatorAddr string
	clientAddr      string
	server          *grpc.Server

	txnStatusMu   sync.RWMutex
	txnStatus     map[string]string
	txnStatusChan map[string]chan string
}

func NewClient(coordinatorAddr string, clientAddr string) *Client {
	return &Client{
		coordinatorAddr: coordinatorAddr,
		clientAddr:      clientAddr,
		txnStatus:       make(map[string]string),
		txnStatusChan:   make(map[string]chan string),
	}
}

func (c *Client) StartServer() error {
	lis, err := net.Listen("tcp", c.clientAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", c.clientAddr, err)
	}

	c.server = grpc.NewServer()
	proto.RegisterClientServiceServer(c.server, c)

	log.Printf("Client server listening on %s", c.clientAddr)

	go func() {
		if err := c.server.Serve(lis); err != nil {
			log.Printf("Client server error: %v", err)
		}
	}()

	return nil
}

func (c *Client) Stop() {
	if c.server != nil {
		c.server.GracefulStop()
	}
}

func (c *Client) ReceiveTxnStatus(ctx context.Context, req *proto.TxnStatusUpdate) (*proto.ClientAck, error) {
	log.Printf("Received transaction status update: %s = %s", req.TxnId, req.Status)

	c.txnStatusMu.Lock()
	c.txnStatus[req.TxnId] = req.Status

	if ch, exists := c.txnStatusChan[req.TxnId]; exists {
		select {
		case ch <- req.Status:
		default:
		}
	}
	c.txnStatusMu.Unlock()

	return &proto.ClientAck{Success: true}, nil
}

func (c *Client) SubmitTransaction(txnID, operation string, accounts []string, amount int32) error {
	conn, err := grpc.Dial(c.coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	client := proto.NewCoordinatorServiceClient(conn)

	req := &proto.TxnRequest{
		Id:         txnID,
		Op:         operation,
		Accounts:   accounts,
		Amount:     amount,
		ClientAddr: c.clientAddr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.Txn(ctx, req)
	if err != nil {
		return fmt.Errorf("transaction submission failed: %v", err)
	}

	if !resp.Accepted {
		return fmt.Errorf("transaction %s was rejected by coordinator", txnID)
	}

	log.Printf("Transaction %s submitted successfully", txnID)
	return nil
}

func (c *Client) TransferMoney(txnID, fromAccount, toAccount string, amount int32) error {
	return c.SubmitTransaction(txnID, "transfer", []string{fromAccount, toAccount}, amount)
}

func (c *Client) CreditAccount(txnID, account string, amount int32) error {
	return c.SubmitTransaction(txnID, "credit", []string{account}, amount)
}

func (c *Client) DebitAccount(txnID, account string, amount int32) error {
	return c.SubmitTransaction(txnID, "debit", []string{account}, amount)
}

func (c *Client) WaitForTransactionStatus(txnID string, timeout time.Duration) (string, error) {
	c.txnStatusMu.Lock()

	if status, exists := c.txnStatus[txnID]; exists {
		c.txnStatusMu.Unlock()
		return status, nil
	}

	statusChan := make(chan string, 1)
	c.txnStatusChan[txnID] = statusChan
	c.txnStatusMu.Unlock()

	defer func() {
		c.txnStatusMu.Lock()
		delete(c.txnStatusChan, txnID)
		c.txnStatusMu.Unlock()
	}()

	select {
	case status := <-statusChan:
		return status, nil
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for transaction %s status", txnID)
	}
}

func (c *Client) GetTransactionStatus(txnID string) (string, error) {
	conn, err := grpc.Dial(c.coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	client := proto.NewCoordinatorServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.GetStatus(ctx, &proto.GetStatusRequest{TxnId: txnID})
	if err != nil {
		return "", fmt.Errorf("failed to get transaction status: %v", err)
	}

	return resp.Status, nil
}

func (c *Client) AcknowledgeTransaction(txnID string) error {
	conn, err := grpc.Dial(c.coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	client := proto.NewCoordinatorServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.AckTxn(ctx, &proto.AckTxnRequest{TxnId: txnID})
	if err != nil {
		return fmt.Errorf("failed to acknowledge transaction: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("transaction acknowledgment failed for %s", txnID)
	}

	log.Printf("Transaction %s acknowledged successfully", txnID)
	return nil
}

func (c *Client) GetAccountBalance(backendAddr, key string) (string, error) {
	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to backend %s: %v", backendAddr, err)
	}
	defer conn.Close()

	client := proto.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, &proto.GetRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("failed to get value: %v", err)
	}

	return resp.Value, nil
}

func (c *Client) SetAccountBalance(backendAddr, key, value string) error {
	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to backend %s: %v", backendAddr, err)
	}
	defer conn.Close()

	client := proto.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Set(ctx, &proto.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to set value: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("set operation failed for key %s", key)
	}

	return nil
}

func (c *Client) ExecuteTransactionWithWait(txnID, operation string, accounts []string, amount int32, timeout time.Duration) (string, error) {
	err := c.SubmitTransaction(txnID, operation, accounts, amount)
	if err != nil {
		return "", err
	}

	status, err := c.WaitForTransactionStatus(txnID, timeout)
	if err != nil {
		return "", err
	}
	if status == "COMMITTED" {
		ackErr := c.AcknowledgeTransaction(txnID)
		if ackErr != nil {
			log.Printf("Failed to acknowledge transaction %s: %v", txnID, ackErr)
		}
	}

	return status, nil
}
