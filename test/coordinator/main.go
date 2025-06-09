package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
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

type TxnStatus string

const (
	TxnPending   TxnStatus = "PENDING"
	TxnPrepared  TxnStatus = "PREPARED"
	TxnCommitted TxnStatus = "COMMITTED"
	TxnAborted   TxnStatus = "ABORTED"
)

type Transaction struct {
	ID         string
	From       string
	To         string
	Amount     int
	ClientAddr string
	Status     TxnStatus
}

var (
	TEST_MODE         = os.Getenv("TEST_MODE") == "true"
	PAUSE_AT_C2       = os.Getenv("PAUSE_AT_C2") == "true"
	PAUSE_AT_C3       = os.Getenv("PAUSE_AT_C3") == "true"
	PAUSE_AT_C4       = os.Getenv("PAUSE_AT_C4") == "true"
	PAUSE_AT_C5       = os.Getenv("PAUSE_AT_C5") == "true"
	PAUSE_AT_C6       = os.Getenv("PAUSE_AT_C6") == "true"
	TEST_SLEEP_MS_ENV = os.Getenv("TEST_SLEEP_MS")
	TEST_SLEEP_MS     = 5000 // Default to 5 seconds if not specified
)

func init() {
	if TEST_SLEEP_MS_ENV != "" {
		if v, err := strconv.Atoi(TEST_SLEEP_MS_ENV); err == nil {
			TEST_SLEEP_MS = v
		}
	}
}

type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer

	TxnQueue       []*Transaction
	TxnMap         map[string]*Transaction
	QueueMu        sync.Mutex
	TxnMu          sync.Mutex
	Timeout        time.Duration
	ProcessChannel chan *Transaction
	NumBins        int
	BinsToBackend  map[string]string
	logPath        string
	logMu          sync.RWMutex
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

	// Send txn to channel for queueing
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

// Load transactions in progress from log file: only one txn will be prepare or pending
// func (c *Coordinator) RecoverTxnLog() *Transaction {
// 	log.Printf("RecoverTxnLog: Starting transaction log recovery from file: %s", c.logPath)

// 	file, err := os.Open(c.logPath)
// 	if err != nil {
// 		log.Printf("RecoverTxnLog: Failed to open log file %s: %v", c.logPath, err)
// 		return nil
// 	}
// 	defer file.Close()
// 	log.Printf("RecoverTxnLog: Successfully opened log file for reading")

// 	scanner := bufio.NewScanner(file)
// 	txnMap := make(map[string]*Transaction)
// 	lineCount := 0

// 	for scanner.Scan() {
// 		lineCount++
// 		line := scanner.Text()
// 		log.Printf("RecoverTxnLog: Processing log line %d: %s", lineCount, line)

// 		fields := strings.Fields(line)
// 		if len(fields) < 3 {
// 			log.Printf("RecoverTxnLog: Skipping malformed line %d (insufficient fields): %s", lineCount, line)
// 			continue
// 		}

// 		txnId := fields[1]
// 		status := fields[2]

// 		txn := txnMap[txnId]
// 		if txn == nil {
// 			txn = &Transaction{ID: txnId}
// 			txnMap[txnId] = txn
// 			log.Printf("RecoverTxnLog: Created new transaction object for ID: %s", txnId)
// 		}

// 		txn.Status = TxnStatus(status)
// 		log.Printf("RecoverTxnLog: Set transaction %s status to %s", txnId, status)
// 	}

// 	if err := scanner.Err(); err != nil {
// 		log.Printf("RecoverTxnLog: Error reading log file: %v", err)
// 		return nil
// 	}

// 	log.Printf("RecoverTxnLog: Processed %d log lines, found %d unique transactions", lineCount, len(txnMap))

// 	// Look for transactions that need recovery
// 	for txnId, txn := range txnMap {
// 		if txn.Status == TxnPrepared || txn.Status == TxnPending {
// 			log.Printf("RecoverTxnLog: Found transaction %s requiring recovery with status %s", txnId, txn.Status)
// 			c.TxnMap[txn.ID] = txn
// 			log.Printf("RecoverTxnLog: Added transaction %s to coordinator's transaction map", txnId)
// 			return txn
// 		}
// 	}

// 	log.Printf("RecoverTxnLog: No transactions found requiring recovery")
// 	return nil
// }

func (c *Coordinator) RecoverTxnLog() []*Transaction {
	log.Printf("RecoverTxnLog: Starting transaction log recovery from file: %s", c.logPath)

	file, err := os.Open(c.logPath)
	if err != nil {
		log.Printf("RecoverTxnLog: Failed to open log file %s: %v", c.logPath, err)
		return nil
	}
	defer file.Close()
	log.Printf("RecoverTxnLog: Successfully opened log file for reading")

	scanner := bufio.NewScanner(file)
	txnMap := make(map[string]*Transaction)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()
		log.Printf("RecoverTxnLog: Processing log line %d: %s", lineCount, line)

		fields := strings.Fields(line)
		if len(fields) < 3 {
			log.Printf("RecoverTxnLog: Skipping malformed line %d (insufficient fields): %s", lineCount, line)
			continue
		}

		txnId := fields[1]
		status := fields[2]

		txn := txnMap[txnId]
		if txn == nil {
			txn = &Transaction{ID: txnId}
			txnMap[txnId] = txn
			log.Printf("RecoverTxnLog: Created new transaction object for ID: %s", txnId)
		}

		txn.Status = TxnStatus(status)
		log.Printf("RecoverTxnLog: Set transaction %s status to %s", txnId, status)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("RecoverTxnLog: Error reading log file: %v", err)
		return nil
	}

	log.Printf("RecoverTxnLog: Processed %d log lines, found %d unique transactions", lineCount, len(txnMap))

	var toRecover []*Transaction
	for txnId, txn := range txnMap {
		if txn.Status == TxnPrepared || txn.Status == TxnPending {
			log.Printf("RecoverTxnLog: Found transaction %s requiring recovery with status %s", txnId, txn.Status)
			c.TxnMap[txn.ID] = txn
			log.Printf("RecoverTxnLog: Added transaction %s to coordinator's transaction map", txnId)
			toRecover = append(toRecover, txn)
		}
	}

	if len(toRecover) == 0 {
		log.Printf("RecoverTxnLog: No transactions found requiring recovery")
	}
	return toRecover
}

// Append to queue file instead of saving full queue
func (c *Coordinator) AppendTxnToQueueFile(txn *Transaction, queuePath string) error {
	log.Printf("AppendTxnToQueueFile: Appending transaction %s to queue file: %s", txn.ID, queuePath)

	f, err := os.OpenFile(queuePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("AppendTxnToQueueFile: Failed to open queue file %s: %v", queuePath, err)
		return err
	}
	defer f.Close()
	log.Printf("AppendTxnToQueueFile: Successfully opened queue file for writing")

	data, err := json.Marshal(txn)
	if err != nil {
		log.Printf("AppendTxnToQueueFile: Failed to marshal transaction %s to JSON: %v", txn.ID, err)
		return err
	}
	log.Printf("AppendTxnToQueueFile: Marshaled transaction %s to JSON, size: %d bytes", txn.ID, len(data))

	_, err = f.WriteString(string(data) + "\n")
	if err != nil {
		log.Printf("AppendTxnToQueueFile: Failed to write transaction %s to queue file: %v", txn.ID, err)
	} else {
		log.Printf("AppendTxnToQueueFile: Successfully appended transaction %s to queue file", txn.ID)
	}

	return err
}

// Recover queue by reading line-by-line appended JSON objects
func (c *Coordinator) RecoverQueueFromFile(queuePath string) error {
	log.Printf("RecoverQueueFromFile: Starting queue recovery from file: %s", queuePath)

	file, err := os.Open(queuePath)
	if err != nil {
		log.Printf("RecoverQueueFromFile: Failed to open queue file %s: %v", queuePath, err)
		return err
	}
	defer file.Close()
	log.Printf("RecoverQueueFromFile: Successfully opened queue file for reading")

	scanner := bufio.NewScanner(file)
	lineCount := 0
	recoveredCount := 0

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()
		log.Printf("RecoverQueueFromFile: Processing queue line %d", lineCount)

		var txn Transaction
		if err := json.Unmarshal([]byte(line), &txn); err != nil {
			log.Printf("RecoverQueueFromFile: Failed to unmarshal line %d: %v", lineCount, err)
			continue
		}

		c.TxnQueue = append(c.TxnQueue, &txn)
		c.TxnMap[txn.ID] = &txn
		recoveredCount++
		log.Printf("RecoverQueueFromFile: Recovered queued transaction %s (total recovered: %d)", txn.ID, recoveredCount)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("RecoverQueueFromFile: Error reading queue file: %v", err)
		return err
	}

	log.Printf("RecoverQueueFromFile: Successfully recovered %d transactions from %d lines in queue file",
		recoveredCount, lineCount)
	return nil
}

// Remove processed transaction from queue.json
func (c *Coordinator) RemoveTxnFromQueueFile(txnId string, queuePath string) error {
	log.Printf("RemoveTxnFromQueueFile: Removing transaction %s from queue file: %s", txnId, queuePath)

	data, err := os.ReadFile(queuePath)
	if err != nil {
		log.Printf("RemoveTxnFromQueueFile: Failed to read queue file %s: %v", queuePath, err)
		return err
	}
	log.Printf("RemoveTxnFromQueueFile: Read queue file, size: %d bytes", len(data))

	lines := strings.Split(string(data), "\n")
	var updated []string
	removedCount := 0

	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			log.Printf("RemoveTxnFromQueueFile: Skipping empty line %d", i+1)
			continue
		}

		var txn Transaction
		if err := json.Unmarshal([]byte(line), &txn); err != nil {
			log.Printf("RemoveTxnFromQueueFile: Failed to unmarshal line %d, keeping as-is: %v", i+1, err)
			updated = append(updated, line)
			continue
		}

		if txn.ID != txnId {
			updated = append(updated, line)
		} else {
			removedCount++
			log.Printf("RemoveTxnFromQueueFile: Removing transaction %s from queue (line %d)", txnId, i+1)
		}
	}

	log.Printf("RemoveTxnFromQueueFile: Removed %d instances of transaction %s from queue", removedCount, txnId)

	err = os.WriteFile(queuePath, []byte(strings.Join(updated, "\n")), 0644)
	if err != nil {
		log.Printf("RemoveTxnFromQueueFile: Failed to write updated queue file: %v", err)
	} else {
		log.Printf("RemoveTxnFromQueueFile: Successfully updated queue file after removing transaction %s", txnId)
	}

	return err
}

// func (c *Coordinator) hasTxnCommittedInLog(txnId string) bool {
// 	log.Printf("hasTxnCommittedInLog: Checking if transaction %s is committed in log", txnId)

// 	data, err := os.ReadFile(c.logPath)
// 	if err != nil {
// 		log.Printf("hasTxnCommittedInLog: Failed to read log file %s: %v", c.logPath, err)
// 		return false
// 	}
// 	log.Printf("hasTxnCommittedInLog: Successfully read log file, size: %d bytes", len(data))

// 	lines := strings.Split(string(data), "\n")
// 	commitPrefix := "Transaction " + txnId + ": COMMITTED"

// 	for i, line := range lines {
// 		if strings.HasPrefix(line, commitPrefix) {
// 			log.Printf("hasTxnCommittedInLog: Found committed transaction %s at line %d: %s", txnId, i+1, line)
// 			return true
// 		}
// 	}

// 	log.Printf("hasTxnCommittedInLog: Transaction %s not found as committed in log", txnId)
// 	return false
// }

func (c *Coordinator) hasTxnCommittedInLog(txnId string) bool {
	log.Printf("hasTxnCommittedInLog: Checking if transaction %s is committed in log", txnId)

	data, err := os.ReadFile(c.logPath)
	if err != nil {
		log.Printf("hasTxnCommittedInLog: Failed to read log file %s: %v", c.logPath, err)
		return false
	}
	log.Printf("hasTxnCommittedInLog: Successfully read log file, size: %d bytes", len(data))

	lines := strings.Split(string(data), "\n")
	commitPrefix := "Transaction " + txnId + ": COMMITTED"

	for i, line := range lines {
		if strings.HasPrefix(line, commitPrefix) {
			log.Printf("hasTxnCommittedInLog: Found committed transaction %s at line %d: %s", txnId, i+1, line)
			return true
		}
	}

	log.Printf("hasTxnCommittedInLog: Transaction %s not found as committed in log", txnId)
	return false
}

func sendAckToClient(addr, txnId, status string) error {
	log.Printf("sendAckToClient: Sending ACK to client %s for transaction %s with status %s", addr, txnId, status)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendAckToClient: Failed to establish connection to client %s: %v", addr, err)
		return err
	}
	defer conn.Close()
	log.Printf("sendAckToClient: Successfully established connection to client %s", addr)

	client := proto.NewClientServiceClient(conn)
	_, err = client.ReceiveTxnStatus(context.Background(), &proto.TxnStatusUpdate{
		TxnId:  txnId,
		Status: status,
	})

	if err != nil {
		log.Printf("sendAckToClient: Failed to send ACK to client %s for transaction %s: %v", addr, txnId, err)
	} else {
		log.Printf("sendAckToClient: Successfully sent ACK to client %s for transaction %s", addr, txnId)
	}

	return err
}

// func (c *Coordinator) QueueWorker() {
// 	log.Printf("QueueWorker: Starting queue worker")

// 	recovered := c.RecoverTxnLog()
// 	if recovered != nil {
// 		log.Printf("QueueWorker: Processing recovered transaction %s", recovered.ID)
// 		c.ProcessTransaction(recovered)
// 	} else {
// 		log.Printf("QueueWorker: No transactions to recover")
// 	}

// 	log.Printf("QueueWorker: Entering main processing loop")
// 	for {
// 		c.QueueMu.Lock()
// 		queueLength := len(c.TxnQueue)

// 		if queueLength == 0 {
// 			c.QueueMu.Unlock()
// 			// log.Printf("QueueWorker: Queue is empty, sleeping for 100ms")
// 			time.Sleep(100 * time.Millisecond)
// 			continue
// 		}

// 		txn := c.TxnQueue[0]
// 		c.TxnQueue = c.TxnQueue[1:]
// 		c.QueueMu.Unlock()

// 		log.Printf("QueueWorker: Dequeued transaction %s (remaining queue length: %d)", txn.ID, queueLength-1)

// 		if c.hasTxnCommittedInLog(txn.ID) {
// 			log.Printf("QueueWorker: Transaction %s already committed, sending commit ACK to %s", txn.ID, txn.ClientAddr)
// 			if err := sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED"); err != nil {
// 				log.Printf("QueueWorker: Failed to send commit ACK for transaction %s: %v", txn.ID, err)
// 			}
// 			continue
// 		}

// 		if txn.Status == TxnPrepared {
// 			log.Printf("QueueWorker: Transaction %s is still in PREPARE state, discarding duplicate", txn.ID)
// 			continue
// 		}

// 		log.Printf("QueueWorker: Processing transaction %s with status %v", txn.ID, txn.Status)
// 		err := c.ProcessTransaction(txn)

// 		if err == nil && txn.Status == TxnCommitted {
// 			log.Printf("QueueWorker: Transaction %s committed successfully, sending ACK to client %s", txn.ID, txn.ClientAddr)
// 			if TEST_MODE && PAUSE_AT_C6 {
// 				log.Printf("TEST HOOK C6: Pausing after CommitPhase success, before sending ACK to client... sleeping %d ms", TEST_SLEEP_MS)
// 				time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
// 			}
// 			if ackErr := sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED"); ackErr != nil {
// 				log.Printf("QueueWorker: Failed to send commit ACK for transaction %s: %v", txn.ID, ackErr)
// 			}
// 		} else {
// 			// Transaction failed or was aborted
// 			if err != nil {
// 				log.Printf("QueueWorker: Transaction %s processing failed: %v", txn.ID, err)
// 			}

// 			// Send failure notification to client
// 			log.Printf("QueueWorker: Transaction %s aborted, sending ABORTED ACK to client %s", txn.ID, txn.ClientAddr)
// 			if ackErr := sendAckToClient(txn.ClientAddr, txn.ID, "ABORTED"); ackErr != nil {
// 				log.Printf("QueueWorker: Failed to send abort ACK for transaction %s: %v", txn.ID, ackErr)
// 			} else {
// 				log.Printf("QueueWorker: Successfully sent ABORTED notification to client for transaction %s", txn.ID)
// 			}
// 		}

// 		log.Printf("QueueWorker: Removing transaction %s from queue file", txn.ID)
// 		if removeErr := c.RemoveTxnFromQueueFile(txn.ID, "queue.json"); removeErr != nil {
// 			log.Printf("QueueWorker: Failed to remove transaction %s from queue file: %v", txn.ID, removeErr)
// 		}
// 	}
// }

func (c *Coordinator) QueueWorker() {
	log.Printf("QueueWorker: Starting queue worker")

	recovered := c.RecoverTxnLog()
	if len(recovered) > 0 {
		for _, txn := range recovered {
			log.Printf("QueueWorker: Processing recovered transaction %s", txn.ID)
			c.ProcessTransaction(txn)
		}
	} else {
		log.Printf("QueueWorker: No transactions to recover")
	}

	log.Printf("QueueWorker: Entering main processing loop")
	for {
		c.QueueMu.Lock()
		queueLength := len(c.TxnQueue)

		if queueLength == 0 {
			c.QueueMu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		txn := c.TxnQueue[0]
		c.TxnQueue = c.TxnQueue[1:]
		c.QueueMu.Unlock()

		log.Printf("QueueWorker: Dequeued transaction %s (remaining queue length: %d)", txn.ID, queueLength-1)

		if c.hasTxnCommittedInLog(txn.ID) {
			log.Printf("QueueWorker: Transaction %s already committed, sending commit ACK to %s", txn.ID, txn.ClientAddr)
			if err := sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED"); err != nil {
				log.Printf("QueueWorker: Failed to send commit ACK for transaction %s: %v", txn.ID, err)
			}
			continue
		}

		if txn.Status == TxnPrepared {
			log.Printf("QueueWorker: Transaction %s is still in PREPARE state, discarding duplicate", txn.ID)
			continue
		}

		log.Printf("QueueWorker: Processing transaction %s with status %v", txn.ID, txn.Status)
		err := c.ProcessTransaction(txn)

		if err == nil && txn.Status == TxnCommitted {
			log.Printf("QueueWorker: Transaction %s committed successfully, sending ACK to client %s", txn.ID, txn.ClientAddr)
			if ackErr := sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED"); ackErr != nil {
				log.Printf("QueueWorker: Failed to send commit ACK for transaction %s: %v", txn.ID, ackErr)
			}
		} else {
			if err != nil {
				log.Printf("QueueWorker: Transaction %s processing failed: %v", txn.ID, err)
			}

			log.Printf("QueueWorker: Transaction %s aborted, sending ABORTED ACK to client %s", txn.ID, txn.ClientAddr)
			if ackErr := sendAckToClient(txn.ClientAddr, txn.ID, "ABORTED"); ackErr != nil {
				log.Printf("QueueWorker: Failed to send abort ACK for transaction %s: %v", txn.ID, ackErr)
			} else {
				log.Printf("QueueWorker: Successfully sent ABORTED notification to client for transaction %s", txn.ID)
			}
		}

		log.Printf("QueueWorker: Removing transaction %s from queue file", txn.ID)
		if removeErr := c.RemoveTxnFromQueueFile(txn.ID, "queue.json"); removeErr != nil {
			log.Printf("QueueWorker: Failed to remove transaction %s from queue file: %v", txn.ID, removeErr)
		}
	}
}
func (c *Coordinator) ChannelWorker() {
	log.Printf("ChannelWorker: Starting channel worker to process incoming transactions")

	for txn := range c.ProcessChannel {
		log.Printf("ChannelWorker: Received transaction %s from process channel", txn.ID)

		c.QueueMu.Lock()
		log.Printf("ChannelWorker: Acquired queue mutex for transaction %s", txn.ID)

		queueLengthBefore := len(c.TxnQueue)
		c.TxnQueue = append(c.TxnQueue, txn)
		log.Printf("ChannelWorker: Added transaction %s to queue (queue length: %d -> %d)",
			txn.ID, queueLengthBefore, len(c.TxnQueue))

		if err := c.AppendTxnToQueueFile(txn, "queue.json"); err != nil {
			log.Printf("ChannelWorker: Failed to append transaction %s to queue file: %v", txn.ID, err)
		} else {
			log.Printf("ChannelWorker: Successfully appended transaction %s to queue file", txn.ID)
		}

		c.QueueMu.Unlock()
		log.Printf("ChannelWorker: Released queue mutex for transaction %s", txn.ID)

		c.TxnMu.Lock()
		log.Printf("ChannelWorker: Acquired transaction mutex for transaction %s", txn.ID)
		c.TxnMap[txn.ID] = txn
		log.Printf("ChannelWorker: Added transaction %s to transaction map", txn.ID)
		c.TxnMu.Unlock()
		log.Printf("ChannelWorker: Released transaction mutex for transaction %s", txn.ID)

		log.Printf("ChannelWorker: Successfully processed transaction %s and added to queue", txn.ID)
	}

	log.Printf("ChannelWorker: Process channel closed, exiting channel worker")
}

// Enhance NewCoordinator to include queue + txn log recovery
func NewCoordinator(BinsJSON string, logPath string, timeout time.Duration, maxNumTransactions int) (*Coordinator, error) {
	log.Printf("NewCoordinator: Creating new coordinator with config - BinsJSON: %s, logPath: %s, timeout: %v, maxTransactions: %d",
		BinsJSON, logPath, timeout, maxNumTransactions)

	coordinator := &Coordinator{
		TxnMap:         make(map[string]*Transaction),
		Timeout:        timeout,
		ProcessChannel: make(chan *Transaction, maxNumTransactions),
		logPath:        logPath,
	}
	log.Printf("NewCoordinator: Initialized coordinator struct with process channel capacity: %d", maxNumTransactions)

	log.Printf("NewCoordinator: Loading bin mapping configuration from %s", BinsJSON)
	if err := coordinator.LoadBinMappingConfig(BinsJSON); err != nil {
		log.Printf("NewCoordinator: Failed to load bin mapping config from %s: %v", BinsJSON, err)
		return nil, fmt.Errorf("could not load %s", BinsJSON)
	}
	log.Printf("NewCoordinator: Successfully loaded bin mapping configuration")

	log.Printf("NewCoordinator: Attempting to recover queue from file: queue.json")
	if err := coordinator.RecoverQueueFromFile("queue.json"); err != nil {
		log.Printf("NewCoordinator: Failed to recover queue from file (continuing anyway): %v", err)
	} else {
		log.Printf("NewCoordinator: Successfully recovered queue from file")
	}

	log.Printf("NewCoordinator: Starting background workers")
	go coordinator.ChannelWorker()
	log.Printf("NewCoordinator: Started ChannelWorker goroutine")

	go coordinator.QueueWorker()
	log.Printf("NewCoordinator: Started QueueWorker goroutine")

	log.Printf("NewCoordinator: Coordinator initialization completed successfully")
	return coordinator, nil
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

	// Check if transaction is in log file
	if c.hasTxnCommittedInLog(req.TxnId) {
		log.Printf("GetStatus: Transaction %s found as committed in log file", req.TxnId)
		return &proto.GetStatusReply{Status: "COMMITTED"}, nil
	}

	log.Printf("GetStatus: Transaction %s not found anywhere, returning UNKNOWN status", req.TxnId)
	return &proto.GetStatusReply{Status: "UNKNOWN"}, nil
}

func (c *Coordinator) LoadBinMappingConfig(path string) error {
	log.Printf("LoadBinMappingConfig: Starting to load configuration from file: %s", path)

	file, err := os.ReadFile(path)
	if err != nil {
		log.Printf("LoadBinMappingConfig: Failed to read config file %s: %v", path, err)
		return err
	}
	log.Printf("LoadBinMappingConfig: Successfully read config file, size: %d bytes", len(file))

	var config struct {
		BinMap  map[string]string `json:"bin_to_backend"`
		NumBins int               `json:"num_bins"`
	}

	if err := json.Unmarshal(file, &config); err != nil {
		log.Printf("LoadBinMappingConfig: Failed to unmarshal JSON from file %s: %v", path, err)
		return err
	}
	log.Printf("LoadBinMappingConfig: Successfully parsed JSON configuration")

	c.BinsToBackend = config.BinMap
	c.NumBins = config.NumBins

	log.Printf("LoadBinMappingConfig: Configuration loaded successfully from %s", path)
	log.Printf("LoadBinMappingConfig: Number of bins configured: %d", c.NumBins)
	log.Printf("LoadBinMappingConfig: Bin mappings loaded: %d entries", len(c.BinsToBackend))

	for bin, addr := range c.BinsToBackend {
		log.Printf("LoadBinMappingConfig: Bin mapping - %s → %s", bin, addr)
	}

	return nil
}

// func (c *Coordinator) LogToFile(entry string) error {
// 	log.Printf("LogToFile: Attempting to log entry to file %s: %s", c.logPath, strings.TrimSpace(entry))

// 	c.logMu.Lock()
// 	defer c.logMu.Unlock()
// 	log.Printf("LogToFile: Acquired log file mutex")

// 	file, err := os.OpenFile(c.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Printf("LogToFile: Failed to open log file %s: %v", c.logPath, err)
// 		return err
// 	}
// 	defer file.Close()
// 	log.Printf("LogToFile: Successfully opened log file for writing")

// 	bytesWritten, err := file.WriteString(entry)
// 	if err != nil {
// 		log.Printf("LogToFile: Failed to write entry to log file: %v", err)
// 		return err
// 	}

// 	if err := file.Sync(); err != nil {
// 		log.Printf("LogToFile: Failed to sync log file: %v", err)
// 		return err
// 	}

// 	log.Printf("LogToFile: Successfully wrote %d bytes to log file %s", bytesWritten, c.logPath)
// 	return nil
// }

func (c *Coordinator) LogToFile(entry string) error {
	log.Printf("LogToFile: Attempting to log entry to file %s: %s", c.logPath, strings.TrimSpace(entry))

	c.logMu.Lock()
	defer c.logMu.Unlock()
	log.Printf("LogToFile: Acquired log file mutex")

	file, err := os.OpenFile(c.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("LogToFile: Failed to open log file %s: %v", c.logPath, err)
		return err
	}
	defer file.Close()
	log.Printf("LogToFile: Successfully opened log file for writing")

	bytesWritten, err := file.WriteString(entry)
	if err != nil {
		log.Printf("LogToFile: Failed to write entry to log file: %v", err)
		return err
	}

	if err := file.Sync(); err != nil {
		log.Printf("LogToFile: Failed to sync log file: %v", err)
		return err
	}

	log.Printf("LogToFile: Successfully wrote %d bytes to log file %s", bytesWritten, c.logPath)
	return nil
}

// Updated ProcessTransaction to support different restart stages with timeout for prepare
func (c *Coordinator) ProcessTransaction(txn *Transaction) error {
	log.Printf("ProcessTransaction: Starting to process transaction %s with current status: %v", txn.ID, txn.Status)

	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	log.Printf("ProcessTransaction: Created context with timeout %v for transaction %s", c.Timeout, txn.ID)

	if txn.Status == TxnPrepared {
		log.Printf("ProcessTransaction: Transaction %s is in PREPARED state, restarting from commit phase", txn.ID)

		if !c.CommitPhase(ctx, txn) {
			log.Printf("ProcessTransaction: Commit phase failed for transaction %s, starting abort", txn.ID)
			txn.Status = TxnAborted
			c.AbortPhase(ctx, txn)
			logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
			if logErr := c.LogToFile(logEntry); logErr != nil {
				log.Printf("ProcessTransaction: Failed to log abort for transaction %s: %v", txn.ID, logErr)
			}
			log.Printf("ProcessTransaction: Transaction %s aborted after commit failure", txn.ID)
			return fmt.Errorf("commit phase failed")
		}

		logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnCommitted)
		if logErr := c.LogToFile(logEntry); logErr != nil {
			log.Printf("ProcessTransaction: Failed to log commit for transaction %s: %v", txn.ID, logErr)
		}
		txn.Status = TxnCommitted
		log.Printf("ProcessTransaction: Transaction %s successfully committed from prepared state", txn.ID)
		return nil

	} else if txn.Status == TxnPending {
		log.Printf("ProcessTransaction: Transaction %s is in PENDING state, restarting from prepare phase", txn.ID)
	} else {
		log.Printf("ProcessTransaction: Transaction %s is new, starting with pending state", txn.ID)
		logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnPending)
		err := c.LogToFile(logEntry)
		if err != nil {
			log.Printf("ProcessTransaction: Failed to log pending state for transaction %s: %v", txn.ID, err)
			txn.Status = TxnAborted
			logEntry = fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
			if abortLogErr := c.LogToFile(logEntry); abortLogErr != nil {
				log.Printf("ProcessTransaction: Failed to log abort after pending failure for transaction %s: %v", txn.ID, abortLogErr)
			}
			return err
		}
		txn.Status = TxnPending
		log.Printf("ProcessTransaction: Transaction %s set to pending state", txn.ID)
	}

	log.Printf("ProcessTransaction: Starting prepare phase for transaction %s", txn.ID)
	done := make(chan bool, 1)
	go func() {
		log.Printf("ProcessTransaction: Prepare phase goroutine started for transaction %s", txn.ID)
		result := c.PreparePhase(ctx, txn)
		log.Printf("ProcessTransaction: Prepare phase completed for transaction %s with result: %t", txn.ID, result)
		done <- result
	}()

	select {
	case success := <-done:
		log.Printf("ProcessTransaction: Prepare phase result received for transaction %s: %t", txn.ID, success)
		if !success {
			log.Printf("ProcessTransaction: Prepare phase failed for transaction %s, starting abort", txn.ID)
			txn.Status = TxnAborted
			c.AbortPhase(ctx, txn)
			logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
			if logErr := c.LogToFile(logEntry); logErr != nil {
				log.Printf("ProcessTransaction: Failed to log abort after prepare failure for transaction %s: %v", txn.ID, logErr)
			}
			log.Printf("ProcessTransaction: Transaction %s aborted after prepare failure", txn.ID)
			return fmt.Errorf("prepare phase failed")
		}
	case <-ctx.Done():
		log.Printf("ProcessTransaction: Timeout waiting for prepare phase for transaction %s, starting abort", txn.ID)
		txn.Status = TxnAborted
		c.AbortPhase(ctx, txn)
		logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
		if logErr := c.LogToFile(logEntry); logErr != nil {
			log.Printf("ProcessTransaction: Failed to log abort after timeout for transaction %s: %v", txn.ID, logErr)
		}
		log.Printf("ProcessTransaction: Transaction %s aborted due to timeout", txn.ID)
		return fmt.Errorf("prepare phase timed out")
	}

	log.Printf("ProcessTransaction: Prepare phase successful for transaction %s, logging prepared state", txn.ID)
	if TEST_MODE && PAUSE_AT_C3 {
		log.Printf("TEST HOOK C3: Pausing after PreparePhase success, before writing TxnPrepared... sleeping %d ms", TEST_SLEEP_MS)
		time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
	}
	logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnPrepared)
	if logErr := c.LogToFile(logEntry); logErr != nil {
		log.Printf("ProcessTransaction: Failed to log prepared state for transaction %s: %v", txn.ID, logErr)
	}
	txn.Status = TxnPrepared
	if TEST_MODE && PAUSE_AT_C4 {
		log.Printf("TEST HOOK C4: Pausing after writing TxnPrepared, before starting CommitPhase... sleeping %d ms", TEST_SLEEP_MS)
		time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
	}
	log.Printf("ProcessTransaction: Starting commit phase for transaction %s", txn.ID)
	if !c.CommitPhase(ctx, txn) {
		log.Printf("ProcessTransaction: Commit phase failed for transaction %s, starting abort", txn.ID)
		txn.Status = TxnAborted
		c.AbortPhase(ctx, txn)
		logEntry = fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
		if logErr := c.LogToFile(logEntry); logErr != nil {
			log.Printf("ProcessTransaction: Failed to log abort after commit failure for transaction %s: %v", txn.ID, logErr)
		}
		log.Printf("ProcessTransaction: Transaction %s aborted after commit failure", txn.ID)
		return fmt.Errorf("commit phase failed")
	}

	log.Printf("ProcessTransaction: Commit phase successful for transaction %s, logging committed state", txn.ID)
	logEntry = fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnCommitted)
	if logErr := c.LogToFile(logEntry); logErr != nil {
		log.Printf("ProcessTransaction: Failed to log committed state for transaction %s: %v", txn.ID, logErr)
	}
	txn.Status = TxnCommitted

	log.Printf("ProcessTransaction: Transaction %s completed successfully with committed status", txn.ID)
	return nil
}

type BatchPrepareRequest struct {
	TxnId      string
	Operations []*proto.PrepareRequest
}

func (c *Coordinator) GetAssociatedBackendsPrepare(txn *Transaction) map[string]*BatchPrepareRequest {
	log.Printf("GetAssociatedBackendsPrepare: Building prepare requests for transaction %s", txn.ID)
	log.Printf("GetAssociatedBackendsPrepare: Transaction details - From: %s, To: %s, Amount: %d",
		txn.From, txn.To, txn.Amount)

	backends := make(map[string]*BatchPrepareRequest)

	if txn.From != "" {
		fromBin := c.hashToBin(txn.From)
		fromBackend := c.BinsToBackend[fromBin]
		fromKey := fmt.Sprintf("%s::%s", fromBin, txn.From)

		debitReq := &proto.PrepareRequest{
			TxnId:     txn.ID,
			Key:       fromKey,
			Value:     fmt.Sprintf("%d", txn.Amount),
			Operation: "DEBIT",
		}

		if _, exists := backends[fromBackend]; !exists {
			backends[fromBackend] = &BatchPrepareRequest{
				TxnId:      txn.ID,
				Operations: []*proto.PrepareRequest{},
			}
		}
		backends[fromBackend].Operations = append(backends[fromBackend].Operations, debitReq)

		log.Printf("GetAssociatedBackendsPrepare: Added DEBIT operation for account %s - bin: %s, backend: %s, key: %s",
			txn.From, fromBin, fromBackend, fromKey)
	} else {
		log.Printf("GetAssociatedBackendsPrepare: No FROM account specified for transaction %s", txn.ID)
	}

	if txn.To != "" {
		toBin := c.hashToBin(txn.To)
		toBackend := c.BinsToBackend[toBin]
		toKey := fmt.Sprintf("%s::%s", toBin, txn.To)

		creditReq := &proto.PrepareRequest{
			TxnId:     txn.ID,
			Key:       toKey,
			Value:     fmt.Sprintf("%d", txn.Amount),
			Operation: "CREDIT",
		}

		if _, exists := backends[toBackend]; !exists {
			backends[toBackend] = &BatchPrepareRequest{
				TxnId:      txn.ID,
				Operations: []*proto.PrepareRequest{},
			}
		}
		backends[toBackend].Operations = append(backends[toBackend].Operations, creditReq)

		log.Printf("GetAssociatedBackendsPrepare: Added CREDIT operation for account %s - bin: %s, backend: %s, key: %s",
			txn.To, toBin, toBackend, toKey)
	} else {
		log.Printf("GetAssociatedBackendsPrepare: No TO account specified for transaction %s", txn.ID)
	}

	// Log total operations per backend
	totalOperations := 0
	for backend, batchReq := range backends {
		operationCount := len(batchReq.Operations)
		totalOperations += operationCount
		log.Printf("GetAssociatedBackendsPrepare: Backend %s will handle %d operations", backend, operationCount)
		for i, op := range batchReq.Operations {
			log.Printf("GetAssociatedBackendsPrepare: Backend %s operation %d: %s on key %s",
				backend, i+1, op.Operation, op.Key)
		}
	}

	log.Printf("GetAssociatedBackendsPrepare: Created %d backend requests with %d total operations for transaction %s",
		len(backends), totalOperations, txn.ID)
	return backends
}

func (c *Coordinator) PreparePhase(ctx context.Context, txn *Transaction) bool {
	log.Printf("PreparePhase: Starting prepare phase for transaction %s", txn.ID)

	backends := c.GetAssociatedBackendsPrepare(txn)
	totalBackends := len(backends)
	log.Printf("PreparePhase: Found %d backends to prepare for transaction %s", totalBackends, txn.ID)

	// Count total operations across all backends
	totalOperations := 0
	for addr, batchReq := range backends {
		operationCount := len(batchReq.Operations)
		totalOperations += operationCount
		log.Printf("PreparePhase: Backend %s will handle %d operations for transaction %s",
			addr, operationCount, txn.ID)
	}

	prepareCh := make(chan bool, totalOperations)
	log.Printf("PreparePhase: Created prepare channel with capacity %d for transaction %s", totalOperations, txn.ID)

	log.Printf("PreparePhase: Launching %d goroutines to send %d prepare requests for transaction %s",
		totalOperations, totalOperations, txn.ID)
	if TEST_MODE && PAUSE_AT_C2 {
		log.Printf("TEST HOOK C2: Pausing after launching Prepare goroutines (before collecting ACKs)... sleeping %d ms", TEST_SLEEP_MS)
		time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
	}
	for backendAddr, batchReq := range backends {
		// Send each operation as a separate prepare request
		for i, prepareReq := range batchReq.Operations {
			go func(addr string, req *proto.PrepareRequest, opIndex int) {
				log.Printf("PreparePhase: Goroutine started for backend %s, operation %d (%s on %s), transaction %s",
					addr, opIndex+1, req.Operation, req.Key, req.TxnId)
				success := c.sendPrepareToBackends(ctx, addr, req)
				log.Printf("PreparePhase: Goroutine completed for backend %s, operation %d, transaction %s, result: %t",
					addr, opIndex+1, req.TxnId, success)
				prepareCh <- success
			}(backendAddr, prepareReq, i)
		}
	}

	successCount := 0
	log.Printf("PreparePhase: Waiting for %d prepare responses for transaction %s", totalOperations, txn.ID)

	for i := 0; i < totalOperations; i++ {
		select {
		case success := <-prepareCh:
			log.Printf("PreparePhase: Received response %d/%d for transaction %s, success: %t",
				i+1, totalOperations, txn.ID, success)
			if success {
				successCount++
				log.Printf("PreparePhase: Success count increased to %d/%d for transaction %s",
					successCount, totalOperations, txn.ID)
			} else {
				log.Printf("PreparePhase: Prepare failed for transaction %s (response %d/%d), aborting phase",
					txn.ID, i+1, totalOperations)
				return false
			}
		case <-ctx.Done():
			log.Printf("PreparePhase: Context timeout/cancellation for transaction %s after %d/%d responses",
				txn.ID, i, totalOperations)
			return false
		}
	}

	allSuccess := successCount == totalOperations
	if allSuccess {
		log.Printf("PreparePhase: All operations prepared successfully for transaction %s (%d/%d)",
			txn.ID, successCount, totalOperations)
	} else {
		log.Printf("PreparePhase: Prepare phase failed for transaction %s (only %d/%d operations succeeded)",
			txn.ID, successCount, totalOperations)
	}

	return allSuccess
}

func (c *Coordinator) sendPrepareToBackends(ctx context.Context, backendAddr string, req *proto.PrepareRequest) bool {
	log.Printf("sendPrepareToBackends: Sending prepare request to backend %s for transaction %s", backendAddr, req.TxnId)
	log.Printf("sendPrepareToBackends: Request details - Key: %s, Value: %s, Operation: %s",
		req.Key, req.Value, req.Operation)

	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendPrepareToBackends: Failed to establish connection to backend %s for transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	defer conn.Close()
	log.Printf("sendPrepareToBackends: Successfully connected to backend %s", backendAddr)

	client := proto.NewKVServiceClient(conn)
	log.Printf("sendPrepareToBackends: Created KV service client for backend %s", backendAddr)

	resp, err := client.Prepare(ctx, req)
	if err != nil {
		log.Printf("sendPrepareToBackends: gRPC call failed for backend %s, key %s, transaction %s: %v",
			backendAddr, req.Key, req.TxnId, err)
		return false
	}
	log.Printf("sendPrepareToBackends: Received response from backend %s for transaction %s", backendAddr, req.TxnId)

	if !resp.Success {
		log.Printf("sendPrepareToBackends: Prepare rejected by backend %s for key %s, transaction %s (Success=false)",
			backendAddr, req.Key, req.TxnId)
		return false
	}

	log.Printf("sendPrepareToBackends: Prepare successful for backend %s, key %s, operation %s, transaction %s",
		backendAddr, req.Key, req.Operation, req.TxnId)
	return true
}

func (c *Coordinator) CommitPhase(ctx context.Context, txn *Transaction) bool {
	log.Printf("CommitPhase: Starting commit phase for transaction %s", txn.ID)

	backends := c.GetAssociatedBackendsCommit(txn)
	totalBackends := len(backends)
	log.Printf("CommitPhase: Found %d backends to commit for transaction %s", totalBackends, txn.ID)

	for addr := range backends {
		log.Printf("CommitPhase: Backend %s will commit transaction %s", addr, txn.ID)
	}

	commitCh := make(chan bool, totalBackends)
	log.Printf("CommitPhase: Created commit channel with capacity %d for transaction %s", totalBackends, txn.ID)

	log.Printf("CommitPhase: Launching %d goroutines to send commit requests for transaction %s", totalBackends, txn.ID)
	if TEST_MODE && PAUSE_AT_C5 {
		log.Printf("TEST HOOK C5: Pausing after launching Commit goroutines (before collecting ACKs)... sleeping %d ms", TEST_SLEEP_MS)
		time.Sleep(time.Duration(TEST_SLEEP_MS) * time.Millisecond)
	}
	for backendAddr, commitReq := range backends {
		go func(addr string, req *proto.CommitRequest) {
			log.Printf("CommitPhase: Goroutine started for backend %s, transaction %s", addr, req.TxnId)
			success := c.sendCommitToBackends(ctx, addr, req)
			log.Printf("CommitPhase: Goroutine completed for backend %s, transaction %s, result: %t", addr, req.TxnId, success)
			commitCh <- success
		}(backendAddr, commitReq)
	}

	successCount := 0
	log.Printf("CommitPhase: Waiting for %d commit responses for transaction %s", totalBackends, txn.ID)

	for i := 0; i < totalBackends; i++ {
		select {
		case success := <-commitCh:
			log.Printf("CommitPhase: Received response %d/%d for transaction %s, success: %t",
				i+1, totalBackends, txn.ID, success)
			if success {
				successCount++
				log.Printf("CommitPhase: Success count increased to %d/%d for transaction %s",
					successCount, totalBackends, txn.ID)
			} else {
				log.Printf("CommitPhase: Commit failed for transaction %s (response %d/%d), aborting phase",
					txn.ID, i+1, totalBackends)
				return false
			}
		case <-ctx.Done():
			log.Printf("CommitPhase: Context timeout/cancellation for transaction %s after %d/%d responses",
				txn.ID, i, totalBackends)
			return false
		}
	}

	allSuccess := successCount == totalBackends
	if allSuccess {
		log.Printf("CommitPhase: All backends committed successfully for transaction %s (%d/%d)",
			txn.ID, successCount, totalBackends)
	} else {
		log.Printf("CommitPhase: Commit phase failed for transaction %s (only %d/%d backends succeeded)",
			txn.ID, successCount, totalBackends)
	}

	return allSuccess
}

func (c *Coordinator) GetAssociatedBackendsCommit(txn *Transaction) map[string]*proto.CommitRequest {
	log.Printf("GetAssociatedBackendsCommit: Building commit requests for transaction %s", txn.ID)
	log.Printf("GetAssociatedBackendsCommit: Transaction details - From: %s, To: %s", txn.From, txn.To)

	backends := make(map[string]*proto.CommitRequest)

	if txn.From != "" {
		fromBin := c.hashToBin(txn.From)
		fromBackend := c.BinsToBackend[fromBin]

		backends[fromBackend] = &proto.CommitRequest{
			TxnId: txn.ID,
		}

		log.Printf("GetAssociatedBackendsCommit: Added commit request for FROM account %s - bin: %s, backend: %s",
			txn.From, fromBin, fromBackend)
	} else {
		log.Printf("GetAssociatedBackendsCommit: No FROM account specified for transaction %s", txn.ID)
	}

	if txn.To != "" {
		toBin := c.hashToBin(txn.To)
		toBackend := c.BinsToBackend[toBin]

		backends[toBackend] = &proto.CommitRequest{
			TxnId: txn.ID,
		}

		log.Printf("GetAssociatedBackendsCommit: Added commit request for TO account %s - bin: %s, backend: %s",
			txn.To, toBin, toBackend)
	} else {
		log.Printf("GetAssociatedBackendsCommit: No TO account specified for transaction %s", txn.ID)
	}

	log.Printf("GetAssociatedBackendsCommit: Created %d commit requests for transaction %s", len(backends), txn.ID)
	return backends
}

func (c *Coordinator) sendCommitToBackends(ctx context.Context, backendAddr string, req *proto.CommitRequest) bool {
	log.Printf("sendCommitToBackends: Sending commit request to backend %s for transaction %s", backendAddr, req.TxnId)

	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendCommitToBackends: Failed to establish connection to backend %s for transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	defer conn.Close()
	log.Printf("sendCommitToBackends: Successfully connected to backend %s", backendAddr)

	client := proto.NewKVServiceClient(conn)
	log.Printf("sendCommitToBackends: Created KV service client for backend %s", backendAddr)

	resp, err := client.Commit(ctx, req)
	if err != nil {
		log.Printf("sendCommitToBackends: gRPC call failed for backend %s, transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	log.Printf("sendCommitToBackends: Received response from backend %s for transaction %s", backendAddr, req.TxnId)

	if !resp.Success {
		log.Printf("sendCommitToBackends: Commit rejected by backend %s for transaction %s (Success=false)",
			backendAddr, req.TxnId)
		return false
	}

	log.Printf("sendCommitToBackends: Commit successful for backend %s, transaction %s", backendAddr, req.TxnId)
	return true
}

func (c *Coordinator) AbortPhase(ctx context.Context, txn *Transaction) {
	log.Printf("AbortPhase: Starting abort phase for transaction %s", txn.ID)

	backends := c.GetAssociatedBackendsAbort(txn)
	totalBackends := len(backends)
	log.Printf("AbortPhase: Found %d backends to abort for transaction %s", totalBackends, txn.ID)

	for addr := range backends {
		log.Printf("AbortPhase: Backend %s will abort transaction %s", addr, txn.ID)
	}

	abortCh := make(chan bool, totalBackends)
	log.Printf("AbortPhase: Created abort channel with capacity %d for transaction %s", totalBackends, txn.ID)

	log.Printf("AbortPhase: Launching %d goroutines to send abort requests for transaction %s", totalBackends, txn.ID)
	for backendAddr, abortReq := range backends {
		go func(addr string, req *proto.AbortRequest) {
			log.Printf("AbortPhase: Goroutine started for backend %s, transaction %s", addr, req.TxnId)
			success := c.sendAbortToBackends(ctx, addr, req)
			log.Printf("AbortPhase: Goroutine completed for backend %s, transaction %s, result: %t", addr, req.TxnId, success)
			abortCh <- success
		}(backendAddr, abortReq)
	}

	successCount := 0
	log.Printf("AbortPhase: Waiting for %d abort responses for transaction %s", totalBackends, txn.ID)

	for i := 0; i < totalBackends; i++ {
		select {
		case success := <-abortCh:
			log.Printf("AbortPhase: Received response %d/%d for transaction %s, success: %t",
				i+1, totalBackends, txn.ID, success)
			if success {
				successCount++
				log.Printf("AbortPhase: Success count increased to %d/%d for transaction %s",
					successCount, totalBackends, txn.ID)
			} else {
				log.Printf("AbortPhase: Abort failed for backend (response %d/%d) for transaction %s",
					i+1, totalBackends, txn.ID)
			}
		case <-ctx.Done():
			log.Printf("AbortPhase: Context timeout/cancellation for transaction %s after %d/%d responses",
				txn.ID, i, totalBackends)
			log.Printf("AbortPhase: Abort phase terminated early for transaction %s (%d/%d backends responded)",
				txn.ID, i, totalBackends)
			return
		}
	}

	log.Printf("AbortPhase: Abort phase completed for transaction %s (%d/%d backends responded successfully)",
		txn.ID, successCount, totalBackends)
}

func (c *Coordinator) GetAssociatedBackendsAbort(txn *Transaction) map[string]*proto.AbortRequest {
	log.Printf("GetAssociatedBackendsAbort: Building abort requests for transaction %s", txn.ID)
	log.Printf("GetAssociatedBackendsAbort: Transaction details - From: %s, To: %s", txn.From, txn.To)

	backends := make(map[string]*proto.AbortRequest)

	if txn.From != "" {
		fromBin := c.hashToBin(txn.From)
		fromBackend := c.BinsToBackend[fromBin]

		backends[fromBackend] = &proto.AbortRequest{
			TxnId: txn.ID,
		}

		log.Printf("GetAssociatedBackendsAbort: Added abort request for FROM account %s - bin: %s, backend: %s",
			txn.From, fromBin, fromBackend)
	} else {
		log.Printf("GetAssociatedBackendsAbort: No FROM account specified for transaction %s", txn.ID)
	}

	if txn.To != "" {
		toBin := c.hashToBin(txn.To)
		toBackend := c.BinsToBackend[toBin]

		backends[toBackend] = &proto.AbortRequest{
			TxnId: txn.ID,
		}

		log.Printf("GetAssociatedBackendsAbort: Added abort request for TO account %s - bin: %s, backend: %s",
			txn.To, toBin, toBackend)
	} else {
		log.Printf("GetAssociatedBackendsAbort: No TO account specified for transaction %s", txn.ID)
	}

	log.Printf("GetAssociatedBackendsAbort: Created %d abort requests for transaction %s", len(backends), txn.ID)
	return backends
}

func (c *Coordinator) sendAbortToBackends(ctx context.Context, backendAddr string, req *proto.AbortRequest) bool {
	log.Printf("sendAbortToBackends: Sending abort request to backend %s for transaction %s", backendAddr, req.TxnId)

	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendAbortToBackends: Failed to establish connection to backend %s for transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	defer conn.Close()
	log.Printf("sendAbortToBackends: Successfully connected to backend %s", backendAddr)

	client := proto.NewKVServiceClient(conn)
	log.Printf("sendAbortToBackends: Created KV service client for backend %s", backendAddr)

	resp, err := client.Abort(ctx, req)
	if err != nil {
		log.Printf("sendAbortToBackends: gRPC call failed for backend %s, transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	log.Printf("sendAbortToBackends: Received response from backend %s for transaction %s", backendAddr, req.TxnId)

	if !resp.Success {
		log.Printf("sendAbortToBackends: Abort rejected by backend %s for transaction %s (Success=false)",
			backendAddr, req.TxnId)
		return false
	}

	log.Printf("sendAbortToBackends: Abort successful for backend %s, transaction %s", backendAddr, req.TxnId)
	return true
}

func (c *Coordinator) hashToBin(account string) string {
	log.Printf("hashToBin: Computing bin for account: %s", account)

	h := fnv.New32a()
	h.Write([]byte(account))
	hashValue := h.Sum32()
	binIndex := int(hashValue) % c.NumBins
	binName := fmt.Sprintf("bin%d", binIndex)

	log.Printf("hashToBin: Account %s -> hash: %d -> bin index: %d -> bin name: %s",
		account, hashValue, binIndex, binName)

	return binName
}
func getCoordinatorAddressFromConfig(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Failed to read config file %s: %v", path, err)
	}

	var config struct {
		Coordinator string `json:"coordinator"`
	}

	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config file %s: %v", path, err)
	}

	return config.Coordinator
}

func main() {
	log.Printf("test_coordinator.go: Main started")

	// Expect --config argument
	if len(os.Args) < 3 || os.Args[1] != "--config" {
		log.Fatalf("Usage: go run test_coordinator.go --config test_config.json")
	}
	configPath := os.Args[2]
	log.Printf("Using config file: %s", configPath)

	// Load config (you already have LoadBinMappingConfig logic)
	coordinator, err := NewCoordinator(configPath, "txn.log", 10*time.Second, 1000)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	// Start gRPC server
	grpcServer := grpc.NewServer()
	proto.RegisterCoordinatorServiceServer(grpcServer, coordinator)

	lis, err := net.Listen("tcp", getCoordinatorAddressFromConfig(configPath))
	if err != nil {
		log.Fatalf("Failed to listen on coordinator address: %v", err)
	}

	log.Printf("Coordinator gRPC server listening on %s", lis.Addr())

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
