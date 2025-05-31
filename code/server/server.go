package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/harshitster/223B-Project/code/proto"
)

type KVServer struct {
	proto.UnimplementedKVServiceServer

	store       map[string]int
	prepare_log map[string]PreparedTxn
	mu          sync.Mutex
	logFilePath string
}

type PreparedTxn struct {
	key   string
	value int
}

func NewKVServer(logFilePath string) *KVServer {
	return &KVServer{
		store:       make(map[string]int),
		prepare_log: make(map[string]PreparedTxn),
		logFilePath: logFilePath,
	}
}

func (s *KVServer) logToFile(entry string) error {
	file, err := os.OpenFile(s.logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(entry)
	return err
}

func (s *KVServer) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.Ack, error) {
	if req.TxnId == "" || req.Key == "" || req.Value == "" {
		return &proto.Ack{Success: false}, fmt.Errorf("transaction ID, key, and value cannot be empty")
	}

	if req.Operation != "DEBIT" && req.Operation != "CREDIT" {
		return &proto.Ack{Success: false}, fmt.Errorf("invalid operation: %s, Must be Debit or Credit", req.Operation)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	txnId := req.TxnId
	key := req.Key
	value := req.Value
	operation := req.Operation

	amount, err := strconv.Atoi(value)
	if err != nil {
		return &proto.Ack{Success: false}, fmt.Errorf("invalid value format: %s must be a number", value)
	}

	currentBalance, exists := s.store[key]
	if !exists {
		currentBalance = 0
	}

	newValue := 0
	switch operation {
	case "DEBIT":
		newValue = currentBalance - amount
		if newValue < 0 {
			return &proto.Ack{Success: false}, fmt.Errorf("insufficient funds: current balance %d, debit amount %d", currentBalance, amount)
		}
	case "CREDIT":
		newValue = currentBalance + amount
	default:
		return &proto.Ack{Success: false}, fmt.Errorf("unknown operation: %s", operation)
	}

	s.prepare_log[txnId] = PreparedTxn{
		key:   key,
		value: newValue,
	}

	entry := fmt.Sprintf("PREPARE %s %s %d\n", txnId, key, newValue)
	err = s.logToFile(entry)

	if err != nil {
		log.Printf("Failed to write prepare message to persistent log: %v", err)
		delete(s.prepare_log, txnId)
		return &proto.Ack{Success: false}, err
	}

	return &proto.Ack{Success: true}, nil
}

func (s *KVServer) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txnId := req.TxnId

	prep, exists := s.prepare_log[txnId]
	if !exists {
		return &proto.Ack{Success: false}, fmt.Errorf("Prepare message for transaction ID '%s' does not exist in memory", txnId)
	}

	s.store[prep.key] = prep.value
	delete(s.prepare_log, txnId)

	logEntry := fmt.Sprintf("COMMIT %s", txnId)
	err := s.logToFile(logEntry)

	return &proto.Ack{Success: true}, err
}

func (s *KVServer) Abort(ctx context.Context, req *proto.AbortRequest) (*proto.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txnId := req.TxnId
	_, exists := s.prepare_log[txnId]
	if exists {
		delete(s.prepare_log, txnId)
		s.logToFile(fmt.Sprintf("ABORT %s", txnId))
	}

	return &proto.Ack{Success: true}, nil
}

func (s *KVServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.ValueReply, error) {
	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.store[req.Key]
	if !exists {
		return &proto.ValueReply{Value: "0"}, nil
	}

	return &proto.ValueReply{Value: strconv.Itoa(value)}, nil
}

func (s *KVServer) Set(ctx context.Context, req *proto.SetRequest) (*proto.Ack, error) {
	if req.Key == "" {
		return &proto.Ack{Success: false}, fmt.Errorf("key cannot be empty")
	}

	value, err := strconv.Atoi(req.Value)
	if err != nil {
		return &proto.Ack{Success: false}, fmt.Errorf("invalid value: must be a number")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[req.Key] = value
	return &proto.Ack{Success: true}, nil
}
