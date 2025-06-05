package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type KVServer struct {
	proto.UnimplementedKVServiceServer

	store              map[string]int
	prepare_log        map[string]PreparedTxn
	mu                 sync.Mutex
	logFilePath        string
	peerAddresses      []string
	selfAddress        string
	coordinatorAddress string
}

type PreparedTxn struct {
	key   string
	value int
}

func NewKVServer(logFilePath string) *KVServer {
	s := &KVServer{
		store:       make(map[string]int),
		prepare_log: make(map[string]PreparedTxn),
		logFilePath: logFilePath,
	}
	// Call recovery inside constructor
	if err := s.RecoverFromLog(); err != nil {
		log.Printf("Warning: failed to recover from log: %v", err)
	} else {
		log.Printf("Recovery from log completed.")
	}

	return s
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

// func (s *KVServer) RecoverFromLog() error {
// 	file, err := os.Open(s.logFilePath)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		fields := strings.Fields(line)
// 		if len(fields) < 2 {
// 			continue
// 		}
// 		switch fields[0] {
// 		case "PREPARE":
// 			txnId, key := fields[1], fields[2]
// 			value, _ := strconv.Atoi(fields[3])
// 			s.prepare_log[txnId] = PreparedTxn{key: key, value: value}
// 		case "COMMIT":
// 			txnId := fields[1]
// 			if prep, ok := s.prepare_log[txnId]; ok {
// 				s.store[prep.key] = prep.value
// 				delete(s.prepare_log, txnId)
// 			}
// 		case "SET":
// 			key := fields[1]
// 			value, _ := strconv.Atoi(fields[2])
// 			s.store[key] = value
// 		case "ABORT":
// 			txnId := fields[1]
// 			delete(s.prepare_log, txnId)
// 		}
// 	}
// 	return scanner.Err()
// }

// func (s *KVServer) RecoverFromLog() error {
// 	file, err := os.Open(s.logFilePath)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		fields := strings.Fields(line)
// 		if len(fields) < 2 {
// 			continue
// 		}
// 		switch fields[0] {
// 		case "PREPARE":
// 			txnId, key := fields[1], fields[2]
// 			value, _ := strconv.Atoi(fields[3])
// 			s.prepare_log[txnId] = PreparedTxn{key: key, value: value}

// 		case "COMMIT":
// 			txnId := fields[1]
// 			if prep, ok := s.prepare_log[txnId]; ok {
// 				s.store[prep.key] = prep.value
// 				delete(s.prepare_log, txnId)
// 			} else {
// 				log.Printf("Warning: COMMIT for txn %s found with no corresponding PREPARE", txnId)
// 			}

// 		case "ABORT":
// 			txnId := fields[1]
// 			delete(s.prepare_log, txnId)

// 		case "DISCARD":
// 			txnId := fields[1]
// 			delete(s.prepare_log, txnId)
// 		}
// 	}

// 	// Restart timeout logic for all recovered prepares
// 	for txnId := range s.prepare_log {
// 		go func(txnId string) {
// 			time.Sleep(5 * time.Second)
// 			status := queryCoordinator(txnId)

// 			if status == "COMMITTED" {
// 				log.Printf("Recovery: Coordinator says COMMITTED for txn %s", txnId)
// 				s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
// 				return
// 			} else if status == "ABORTED" {
// 				log.Printf("Recovery: Coordinator says ABORTED for txn %s", txnId)
// 				s.Abort(context.Background(), &proto.AbortRequest{TxnId: txnId})
// 				return
// 			}

// 			// Query peers as fallback
// 			result := queryBackendsForTxnStatus(txnId, s.peerAddresses, s.selfAddress)
// 			if result == "COMMITTED" {
// 				log.Printf("Recovery: Peer confirms COMMITTED for txn %s", txnId)
// 				s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
// 			} else {
// 				log.Printf("Recovery: Txn %s discarded after no confirmation", txnId)
// 				s.mu.Lock()
// 				delete(s.prepare_log, txnId)
// 				s.mu.Unlock()
// 				s.logToFile(fmt.Sprintf("DISCARD %s\n", txnId))
// 			}
// 		}(txnId)
// 	}

// 	return scanner.Err()
// }

func (s *KVServer) PostPrepare(txnId string) {
	status := s.queryCoordinator(txnId)

	if status == "COMMITTED" {
		log.Printf("Coordinator says COMMITTED for txn %s", txnId)
		s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
		return
	} else if status == "ABORTED" {
		log.Printf("Coordinator says ABORTED for txn %s", txnId)
		s.Abort(context.Background(), &proto.AbortRequest{TxnId: txnId})
		return
	}

	result := s.queryBackendsForTxnStatus(txnId)
	if result == "COMMITTED" {
		log.Printf("Peer confirms COMMITTED for txn %s", txnId)
		s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
	} else if result == "UNKNOWN" {
		log.Printf("Txn %s discarded after timeout", txnId)
		s.mu.Lock()
		delete(s.prepare_log, txnId)
		s.mu.Unlock()
		s.logToFile(fmt.Sprintf("DISCARD %s\n", txnId))
	}
}
func (s *KVServer) RecoverFromLog() error {
	file, err := os.Open(s.logFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "PREPARE":
			txnId, key := fields[1], fields[2]
			value, _ := strconv.Atoi(fields[3])
			s.prepare_log[txnId] = PreparedTxn{key: key, value: value}
		case "COMMIT":
			txnId := fields[1]
			if prep, ok := s.prepare_log[txnId]; ok {
				s.store[prep.key] = prep.value
				delete(s.prepare_log, txnId)
			} else {
				log.Printf("Warning: COMMIT for txn %s found with no corresponding PREPARE", txnId)
			}
		case "ABORT":
			txnId := fields[1]
			delete(s.prepare_log, txnId)
		case "DISCARD":
			txnId := fields[1]
			delete(s.prepare_log, txnId)
		default:
			log.Printf("Unknown log action: %q", line)
		}
	}

	for txnId := range s.prepare_log {
		go s.PostPrepare(txnId)
	}

	return scanner.Err()
}

// func queryCoordinator(txnId string) string {
// 	conn, err := grpc.Dial(coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		log.Printf("Failed to connect to coordinator: %v", err)
// 		return "UNKNOWN"
// 	}
// 	defer conn.Close()

// 	client := proto.NewCoordinatorServiceClient(conn)
// 	resp, err := client.GetStatus(context.Background(), &proto.GetStatusRequest{TxnId: txnId})
// 	if err != nil {
// 		log.Printf("Coordinator GetStatus failed: %v", err)
// 		return "UNKNOWN"
// 	}
// 	return resp.Status
// }

func (s *KVServer) queryCoordinator(txnId string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.Dial(s.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to coordinator: %v", err)
		return "UNKNOWN"
	}
	defer conn.Close()

	client := proto.NewCoordinatorServiceClient(conn)

	respCh := make(chan string, 1)

	go func() {
		resp, err := client.GetStatus(ctx, &proto.GetStatusRequest{TxnId: txnId})
		if err != nil {
			respCh <- "UNKNOWN"
			return
		}
		respCh <- resp.Status
	}()

	select {
	case <-ctx.Done():
		log.Printf("Timeout while querying coordinator for txn %s", txnId)
		return "UNKNOWN"
	case status := <-respCh:
		return status
	}
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

	if _, exists := s.prepare_log[req.TxnId]; exists {
		return &proto.Ack{Success: true}, nil
	}

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

	go s.PostPrepare(txnId)

	return &proto.Ack{Success: true}, nil
}

// func (s *KVServer) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.Ack, error) {
// 	txnId := req.TxnId
// 	key := req.Key
// 	value := req.Value
// 	operation := req.Operation

// 	if _, exists := s.prepare_log[txnId]; exists {
// 		return &proto.Ack{Success: true}, nil // Already prepared
// 	}
// 	if req.TxnId == "" || req.Key == "" || req.Value == "" {
// 		return &proto.Ack{Success: false}, fmt.Errorf("transaction ID, key, and value cannot be empty")
// 	}

// 	if req.Operation != "DEBIT" && req.Operation != "CREDIT" {
// 		return &proto.Ack{Success: false}, fmt.Errorf("invalid operation: %s, Must be Debit or Credit", req.Operation)
// 	}

// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	amount, err := strconv.Atoi(value)
// 	if err != nil {
// 		return &proto.Ack{Success: false}, fmt.Errorf("invalid value format: %s must be a number", value)
// 	}

// 	currentBalance, exists := s.store[key]
// 	if !exists {
// 		currentBalance = 0
// 	}

// 	newValue := 0
// 	switch operation {
// 	case "DEBIT":
// 		newValue = currentBalance - amount
// 		if newValue < 0 {
// 			return &proto.Ack{Success: false}, fmt.Errorf("insufficient funds: current balance %d, debit amount %d", currentBalance, amount)
// 		}
// 	case "CREDIT":
// 		newValue = currentBalance + amount
// 	default:
// 		return &proto.Ack{Success: false}, fmt.Errorf("unknown operation: %s", operation)
// 	}

// 	s.prepare_log[txnId] = PreparedTxn{
// 		key:   key,
// 		value: newValue,
// 	}

// 	entry := fmt.Sprintf("PREPARE %s %s %d\n", txnId, key, newValue)
// 	err = s.logToFile(entry)

// 	if err != nil {
// 		log.Printf("Failed to write prepare message to persistent log: %v", err)
// 		delete(s.prepare_log, txnId)
// 		return &proto.Ack{Success: false}, err
// 	}
// 	go func(txnId string) {
// 		time.Sleep(5 * time.Second)
// 		status := queryCoordinator(txnId)

// 		if status == "COMMITTED" {
// 			log.Printf("Coordinator says COMMITTED for txn %s", txnId)
// 			s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
// 			return
// 		} else if status == "ABORTED" {
// 			log.Printf("Coordinator says ABORTED for txn %s", txnId)
// 			s.Abort(context.Background(), &proto.AbortRequest{TxnId: txnId})
// 			return
// 		}

// 		// Query peers
// 		result := queryBackendsForTxnStatus(txnId, s.peerAddresses, s.selfAddress)
// 		if result == "COMMITTED" {
// 			log.Printf("Peer confirms COMMITTED for txn %s", txnId)
// 			s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
// 		} else if result == "UNKNOWN" {
// 			log.Printf("Txn %s discarded after timeout", txnId)
// 			s.mu.Lock()
// 			delete(s.prepare_log, txnId)
// 			s.mu.Unlock()
// 			s.logToFile(fmt.Sprintf("DISCARD %s\n", txnId))
// 		}
// 	}(txnId)

// 	return &proto.Ack{Success: true}, nil
// }

// QueryTxnStatus RPC handler
// func (s *KVServer) QueryTxnStatus(ctx context.Context, req *proto.QueryTxnRequest) (*proto.QueryTxnReply, error) {
// 	txnId := req.TxnId

// 	s.mu.Lock()
// 	_, prepared := s.prepare_log[txnId]
// 	s.mu.Unlock()
// 	if prepared {
// 		return &proto.QueryTxnReply{Status: "PREPARED"}, nil
// 	}

// 	// Fallback to check logs for COMMIT or DISCARD
// 	data, err := os.ReadFile(s.logFilePath)
// 	if err == nil {
// 		lines := strings.Split(string(data), "\n")
// 		for _, line := range lines {
// 			if strings.HasPrefix(line, "COMMIT "+txnId) {
// 				return &proto.QueryTxnReply{Status: "COMMITTED"}, nil
// 			}
// 			if strings.HasPrefix(line, "DISCARD "+txnId) {
// 				return &proto.QueryTxnReply{Status: "DISCARDED"}, nil
// 			}
// 		}
// 	}

//		return &proto.QueryTxnReply{Status: "UNKNOWN"}, nil
//	}
func (s *KVServer) QueryTxnStatus(ctx context.Context, req *proto.QueryTxnRequest) (*proto.QueryTxnReply, error) {
	txnId := req.TxnId

	s.mu.Lock()
	_, prepared := s.prepare_log[txnId]
	s.mu.Unlock()
	if prepared {
		return &proto.QueryTxnReply{Status: "PREPARED"}, nil
	}

	data, err := os.ReadFile(s.logFilePath)
	if err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "COMMIT "+txnId) {
				return &proto.QueryTxnReply{Status: "COMMITTED"}, nil
			}
			if strings.HasPrefix(line, "DISCARD "+txnId) {
				return &proto.QueryTxnReply{Status: "DISCARDED"}, nil
			}
			if strings.HasPrefix(line, "ABORT "+txnId) {
				return &proto.QueryTxnReply{Status: "ABORTED"}, nil
			}
		}
	}

	return &proto.QueryTxnReply{Status: "UNKNOWN"}, nil
}

// Query all peers
// func queryBackendsForTxnStatus(txnId string, peers []string, selfAddr string) string {
// 	for _, addr := range peers {
// 		if addr == selfAddr {
// 			continue
// 		}
// 		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 		if err != nil {
// 			continue
// 		}
// 		client := proto.NewKVServiceClient(conn)
// 		resp, err := client.QueryTxnStatus(context.Background(), &proto.QueryTxnRequest{TxnId: txnId})
// 		conn.Close()
// 		if err != nil {
// 			continue
// 		}
// 		if resp.Status == "COMMITTED" {
// 			return "COMMITTED"
// 		}
// 	}
// 	return "UNKNOWN"
// }

func (s *KVServer) queryBackendsForTxnStatus(txnId string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultCh := make(chan string, len(s.peerAddresses))

	for _, addr := range s.peerAddresses {
		if addr == s.selfAddress {
			continue
		}

		go func(peerAddr string) {
			conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				resultCh <- "UNKNOWN"
				return
			}
			defer conn.Close()

			client := proto.NewKVServiceClient(conn)
			resp, err := client.QueryTxnStatus(ctx, &proto.QueryTxnRequest{TxnId: txnId})
			if err != nil {
				resultCh <- "UNKNOWN"
				return
			}
			resultCh <- resp.Status
		}(addr)
	}

	expected := len(s.peerAddresses) - 1
	received := 0
	anyCommitted := false

	for {
		select {
		case status := <-resultCh:
			received++
			if status == "COMMITTED" {
				anyCommitted = true
			}
			if received == expected {
				if anyCommitted {
					return "COMMITTED"
				}
				return "UNKNOWN"
			}
		case <-ctx.Done():
			log.Printf("Timeout while querying peers for txn %s", txnId)
			if anyCommitted {
				return "COMMITTED"
			}
			return "UNKNOWN"
		}
	}
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

	logEntry := fmt.Sprintf("COMMIT %s\n", txnId)
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
