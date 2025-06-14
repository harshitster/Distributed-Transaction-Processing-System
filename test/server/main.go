package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var TEST_MODE = os.Getenv("TEST_MODE") == "true"
var TEST_SLEEP_MS_ENV = os.Getenv("TEST_SLEEP_MS")
var TEST_SLEEP_MS = 0

var PAUSE_AT_S2 = os.Getenv("PAUSE_AT_S2") == "true"
var PAUSE_AT_S4 = os.Getenv("PAUSE_AT_S4") == "true"
var PAUSE_AT_S5 = os.Getenv("PAUSE_AT_S5") == "true"
var PAUSE_AT_S6 = os.Getenv("PAUSE_AT_S6") == "true"

func init() {
	if ms, err := strconv.Atoi(TEST_SLEEP_MS_ENV); err == nil {
		TEST_SLEEP_MS = ms
	}
}

type KVServer struct {
	proto.UnimplementedKVServiceServer

	store              map[string]int
	prepare_log        map[string]map[string]PreparedTxn // txnId -> accountKey -> PreparedTxn
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

func NewKVServer(logFilePath string, peerAddresses []string, selfAddress string, coordinatorAddress string) *KVServer {
	s := &KVServer{
		store:              make(map[string]int),
		prepare_log:        make(map[string]map[string]PreparedTxn),
		logFilePath:        logFilePath,
		peerAddresses:      peerAddresses,
		selfAddress:        selfAddress,
		coordinatorAddress: coordinatorAddress,
	}
	if err := s.RecoverFromLog(); err != nil {
		log.Printf("Warning: failed to recover from log: %v", err)
	} else {
		log.Printf("Recovery from log completed.")
	}

	return s
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
			if len(fields) < 4 {
				continue
			}
			txnId, key := fields[1], fields[2]
			value, _ := strconv.Atoi(fields[3])

			if s.prepare_log[txnId] == nil {
				s.prepare_log[txnId] = make(map[string]PreparedTxn)
			}
			s.prepare_log[txnId][key] = PreparedTxn{key: key, value: value}
		case "COMMIT":
			txnId := fields[1]
			if txnOps, ok := s.prepare_log[txnId]; ok {
				for _, prep := range txnOps {
					s.store[prep.key] = prep.value
				}
				delete(s.prepare_log, txnId)
			} else {
				log.Printf("Warning: COMMIT for txn %s found with no corresponding PREPARE", txnId)
			}
		case "ABORT":
			if len(fields) >= 4 {
				// New format: ABORT txnId key value
				txnId := fields[1]
				delete(s.prepare_log, txnId)
			} else {
				// Old format: ABORT txnId (for backward compatibility)
				txnId := fields[1]
				delete(s.prepare_log, txnId)
			}
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

	// Check if this specific operation for this transaction already exists
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

	// Log current store contents for debugging
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

	// Initialize transaction map if it doesn't exist
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
	if TEST_MODE && PAUSE_AT_S2 {
		log.Printf("TEST HOOK S2: Pausing after writing PREPARE to log but before sending ACK...")
		time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
	}
	// Only start PostPrepare goroutine once per transaction
	if len(s.prepare_log[txnId]) == 1 {
		log.Printf("PREPARE: Starting PostPrepare goroutine for txnId: %s", txnId)
		go s.PostPrepare(txnId)
	} else {
		log.Printf("PREPARE: PostPrepare already running for txnId: %s (operation count: %d)", txnId, len(s.prepare_log[txnId]))
	}

	log.Printf("PREPARE SUCCESS: Transaction %s prepared successfully for key %s", txnId, key)
	return &proto.Ack{Success: true}, nil
}

func (s *KVServer) PostPrepare(txnId string) {
	time.Sleep(10 * time.Second)

	// Step 1: Check own log file for COMMITTED entry
	log.Printf("PostPrepare: Checking local log file %s for transaction %s", s.logFilePath, txnId)
	data, err := os.ReadFile(s.logFilePath)
	if err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.Contains(line, fmt.Sprintf("COMMIT %s", txnId)) {
				log.Printf("PostPrepare: Found COMMIT entry in local log for txn %s, exiting", txnId)
				return
			}
		}
	} else {
		log.Printf("PostPrepare: Failed to read log file %s: %v", s.logFilePath, err)
	}

	// Step 2: Query coordinator
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

	// Step 3: Fallback to peer query
	result := s.queryBackendsForTxnStatus(txnId)
	if result == "COMMITTED" {
		log.Printf("Peer confirms COMMITTED for txn %s", txnId)
		s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
	} else if result == "UNKNOWN" {
		log.Printf("Txn %s discarded after timeout", txnId)
		s.mu.Lock()
		txnOps, exists := s.prepare_log[txnId]
		if exists {
			for key, prep := range txnOps {
				discardEntry := fmt.Sprintf("DISCARD %s %s %d\n", txnId, key, prep.value)
				s.logToFile(discardEntry)
			}
			delete(s.prepare_log, txnId)
		}
		s.mu.Unlock()
	}
}

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

func (s *KVServer) queryBackendsForTxnStatus(txnId string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Initiated Query Backends for txn id: %s", txnId)

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

func (s *KVServer) queryCoordinator(txnId string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Initiated Query Coordinator for txn id: %s", txnId)

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
	os.Stdout.Sync()
	os.Stderr.Sync()
	// Apply all operations for this transaction
	for key, prep := range txnOps {
		oldValue := s.store[prep.key]
		s.store[prep.key] = prep.value
		log.Printf("COMMIT: Applied operation - key: %s, old value: %d, new value: %d", prep.key, oldValue, prep.value)
		if TEST_MODE && PAUSE_AT_S4 {
			log.Printf("TEST HOOK S4: Pausing after applying key %s...", prep.key)
			time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
		}
		// Log each operation being committed
		commitEntry := fmt.Sprintf("COMMIT %s %s %d\n", txnId, key, prep.value)
		log.Printf("COMMIT: Writing to log file %s: %s", s.logFilePath, strings.TrimSpace(commitEntry))
		s.logToFile(commitEntry)
	}

	delete(s.prepare_log, txnId)
	if TEST_MODE && PAUSE_AT_S5 {
		log.Printf("TEST HOOK S5: Pausing after full Commit written and prepare_log cleared...")
		time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
	}
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

		// Log each operation being aborted
		for key, prep := range txnOps {
			abortEntry := fmt.Sprintf("ABORT %s %s %d\n", txnId, key, prep.value)
			log.Printf("ABORT: Writing to log file %s: %s", s.logFilePath, strings.TrimSpace(abortEntry))
			s.logToFile(abortEntry)

		}
		if TEST_MODE && PAUSE_AT_S6 {
			log.Printf("TEST HOOK S6: Pausing after writing ABORT to log but before sending ACK...")
			time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
		}
		delete(s.prepare_log, txnId)
		log.Printf("ABORT: Successfully aborted transaction %s", txnId)
	} else {
		log.Printf("ABORT: Transaction %s not found in prepare_log. No issues.", txnId)
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

func main() {
	log.Printf("test_server.go: Main started")

	// Expect: --config CONFIG --backend BACKEND_ID
	if len(os.Args) < 5 || os.Args[1] != "--config" || os.Args[3] != "--backend" {
		log.Fatalf("Usage: go run test_server.go --config test_config.json --backend backendX")
	}

	configPath := os.Args[2]
	backendID := os.Args[4]

	log.Printf("Using config file: %s", configPath)
	log.Printf("Using backend ID: %s", backendID)

	// Load config
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read config file %s: %v", configPath, err)
	}

	var config struct {
		Coordinator string            `json:"coordinator"`
		ClientAddr  string            `json:"client_address"`
		BackendMap  map[string]string `json:"backend_map"`
	}

	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config file %s: %v", configPath, err)
	}

	selfAddress, ok := config.BackendMap[backendID]
	if !ok {
		log.Fatalf("Backend ID %s not found in config", backendID)
	}

	// Build peer addresses
	var peerAddresses []string
	for _, addr := range config.BackendMap {
		peerAddresses = append(peerAddresses, addr)
	}

	// Build KVServer
	logFilePath := fmt.Sprintf("%s.log", backendID)

	kvServer := NewKVServer(logFilePath, peerAddresses, selfAddress, config.Coordinator)

	// Start gRPC server
	grpcServer := grpc.NewServer()
	proto.RegisterKVServiceServer(grpcServer, kvServer)

	lis, err := net.Listen("tcp", selfAddress)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", selfAddress, err)
	}

	log.Printf("KVServer gRPC server for %s listening on %s", backendID, lis.Addr())

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
