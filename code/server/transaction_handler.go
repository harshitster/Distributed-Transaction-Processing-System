package server

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/harshitster/223B-Project/code/proto"
)

func (s *KVServer) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.Ack, error) {
	log.Printf("=== PREPARE CALLED ===")
	log.Printf("PREPARE: TxnId: %s", req.TxnId)
	log.Printf("PREPARE: Key: %s", req.Key)
	log.Printf("PREPARE: Value: %s", req.Value)
	log.Printf("PREPARE: Operation: %s", req.Operation)

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

	if txnOps, exists := s.prepare_log[txnId]; exists {
		if _, opExists := txnOps[key]; opExists {
			log.Printf("PREPARE: Operation for transaction %s and key %s already exists, returning success", txnId, key)
			return &proto.Ack{Success: true}, nil
		}
	}

	amount, err := strconv.Atoi(value)
	if err != nil {
		return &proto.Ack{Success: false}, fmt.Errorf("invalid value format: %s must be a number", value)
	}

	currentBalance, exists := s.store[key]
	if !exists {
		log.Printf("PREPARE: Key %s does not exist, defaulting balance to 0", key)
		currentBalance = 0
	} else {
		log.Printf("PREPARE: Current balance for key %s: %d", key, currentBalance)
	}

	log.Printf("PREPARE: Current store contents:")
	for k, v := range s.store {
		log.Printf("  %s: %d", k, v)
	}

	newValue := 0
	switch operation {
	case "DEBIT":
		newValue = currentBalance - amount
		log.Printf("PREPARE: DEBIT calculation: %d - %d = %d", currentBalance, amount, newValue)
		if newValue < 0 {
			log.Printf("PREPARE: DEBIT failed - insufficient funds: current balance %d, debit amount %d", currentBalance, amount)
			return &proto.Ack{Success: false}, fmt.Errorf("insufficient funds: current balance %d, debit amount %d", currentBalance, amount)
		}
	case "CREDIT":
		newValue = currentBalance + amount
		log.Printf("PREPARE: CREDIT calculation: %d + %d = %d", currentBalance, amount, newValue)
	default:
		return &proto.Ack{Success: false}, fmt.Errorf("unknown operation: %s", operation)
	}

	if s.prepare_log[txnId] == nil {
		s.prepare_log[txnId] = make(map[string]PreparedTxn)
		log.Printf("PREPARE: Created new transaction entry for txnId: %s", txnId)
	}

	s.prepare_log[txnId][key] = PreparedTxn{
		key:   key,
		value: newValue,
	}
	log.Printf("PREPARE: Stored in prepare_log - txnId: %s, key: %s, value: %d", txnId, key, newValue)

	entry := fmt.Sprintf("PREPARE %s %s %d\n", txnId, key, newValue)
	log.Printf("PREPARE: Writing to log file %s: %s", s.logFilePath, strings.TrimSpace(entry))
	err = s.logToFile(entry)

	if err != nil {
		log.Printf("PREPARE: Failed to write prepare message to persistent log: %v", err)
		delete(s.prepare_log[txnId], key)
		if len(s.prepare_log[txnId]) == 0 {
			delete(s.prepare_log, txnId)
		}
		return &proto.Ack{Success: false}, err
	}

	if len(s.prepare_log[txnId]) == 1 {
		log.Printf("PREPARE: Starting PostPrepare goroutine for txnId: %s", txnId)
		go s.PostPrepare(txnId)
	} else {
		log.Printf("PREPARE: PostPrepare already running for txnId: %s (operation count: %d)", txnId, len(s.prepare_log[txnId]))
	}

	log.Printf("PREPARE SUCCESS: Transaction %s prepared successfully for key %s", txnId, key)
	return &proto.Ack{Success: true}, nil
}

func (s *KVServer) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Ack, error) {
	log.Printf("=== COMMIT CALLED ===")
	log.Printf("COMMIT: TxnId: %s", req.TxnId)

	s.mu.Lock()
	defer s.mu.Unlock()

	txnId := req.TxnId

	txnOps, exists := s.prepare_log[txnId]
	if !exists {
		log.Printf("COMMIT: Transaction %s not found in prepare_log", txnId)
		return &proto.Ack{Success: false}, fmt.Errorf("Prepare message for transaction ID '%s' does not exist in memory", txnId)
	}

	log.Printf("COMMIT: Found transaction %s with %d operations to commit", txnId, len(txnOps))

	for key, prep := range txnOps {
		oldValue := s.store[prep.key]
		s.store[prep.key] = prep.value
		log.Printf("COMMIT: Applied operation - key: %s, old value: %d, new value: %d", prep.key, oldValue, prep.value)

		commitEntry := fmt.Sprintf("COMMIT %s %s %d\n", txnId, key, prep.value)
		log.Printf("COMMIT: Writing to log file %s: %s", s.logFilePath, strings.TrimSpace(commitEntry))
		s.logToFile(commitEntry)
	}

	delete(s.prepare_log, txnId)
	log.Printf("COMMIT: Successfully committed transaction %s", txnId)

	return &proto.Ack{Success: true}, nil
}

func (s *KVServer) Abort(ctx context.Context, req *proto.AbortRequest) (*proto.Ack, error) {
	log.Printf("=== ABORT CALLED ===")
	log.Printf("ABORT: TxnId: %s", req.TxnId)

	s.mu.Lock()
	defer s.mu.Unlock()

	txnId := req.TxnId
	txnOps, exists := s.prepare_log[txnId]
	if exists {
		log.Printf("ABORT: Found transaction %s with %d operations to abort", txnId, len(txnOps))

		for key, prep := range txnOps {
			abortEntry := fmt.Sprintf("ABORT %s %s %d\n", txnId, key, prep.value)
			log.Printf("ABORT: Writing to log file %s: %s", s.logFilePath, strings.TrimSpace(abortEntry))
			s.logToFile(abortEntry)
		}

		delete(s.prepare_log, txnId)
		log.Printf("ABORT: Successfully aborted transaction %s", txnId)
	} else {
		log.Printf("ABORT: Transaction %s not found in prepare_log. No issues.", txnId)
	}

	return &proto.Ack{Success: true}, nil
}
