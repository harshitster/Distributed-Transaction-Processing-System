package coordinator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
	// Deduplication: If already committed, reject duplicate
	c.TxnMu.Lock()
	if existing, ok := c.TxnMap[req.Id]; ok {
		if existing.Status == TxnCommitted {
			c.TxnMu.Unlock()
			log.Printf("Txn %s is already committed", req.Id)
			return &proto.TxnResponse{Accepted: false}, nil
		}
		c.TxnMu.Unlock()
		log.Printf("Txn %s is a duplicate in-flight transaction", req.Id)
		return &proto.TxnResponse{Accepted: false}, nil
	}
	c.TxnMu.Unlock()

	txn := &Transaction{
		ID:         req.Id,
		Amount:     int(req.Amount),
		ClientAddr: req.ClientAddr,
	}

	switch req.Op {
	case "transfer":
		if len(req.Accounts) < 2 {
			return &proto.TxnResponse{Accepted: false}, fmt.Errorf("transfer requires two accounts")
		}
		txn.From = req.Accounts[0]
		txn.To = req.Accounts[1]

	case "credit":
		if len(req.Accounts) < 1 {
			return &proto.TxnResponse{Accepted: false}, fmt.Errorf("credit requires one account")
		}
		txn.To = req.Accounts[0]

	case "debit":
		if len(req.Accounts) < 1 {
			return &proto.TxnResponse{Accepted: false}, fmt.Errorf("debit requires one account")
		}
		txn.From = req.Accounts[0]

	default:
		return &proto.TxnResponse{Accepted: false}, fmt.Errorf("unsupported operation: %s", req.Op)
	}

	// Send txn to channel for queueing
	select {
	case c.ProcessChannel <- txn:
		return &proto.TxnResponse{Accepted: true}, nil
	case <-ctx.Done():
		return &proto.TxnResponse{Accepted: false}, fmt.Errorf("context cancelled or timed out")
	}
}

// func (c *Coordinator) AckTxn(txnId string) (*proto.CoordAck, error) {
// 	c.TxnMu.Lock()
// 	defer c.TxnMu.Unlock()

// 	txn, ok := c.TxnMap[txnId]
// 	if !ok || txn.Status != TxnCommitted {
// 		return &proto.CoordAck{Success: false}, nil
// 	}

// 	log.Printf("Client acknowledged committed txn %s", txnId)
// 	delete(c.TxnMap, txnId)

// 	// Remove txn from log
// 	data, err := os.ReadFile(c.logPath)
// 	if err != nil {
// 		log.Printf("Failed to read log file for cleanup: %v", err)
// 		return &proto.CoordAck{Success: false}, nil
// 	}

// 	lines := strings.Split(string(data), "\n")
// 	var updated []string
// 	for _, line := range lines {
// 		// Match lines like: "Transaction abc123: COMMITTED"
// 		if !strings.Contains(line, fmt.Sprintf("Transaction %s:", txnId)) {
// 			updated = append(updated, line)
// 		}
// 	}

// 	err = os.WriteFile(c.logPath, []byte(strings.Join(updated, "\n")), 0644)
// 	if err != nil {
// 		log.Printf("Failed to write updated log after removing txn %s: %v", txnId, err)
// 	} else {
// 		log.Printf("Txn %s removed from log after client ACK", txnId)
// 	}
// 	return &proto.CoordAck{Success: true}, nil
// }

func (c *Coordinator) AckTxn(ctx context.Context, req *proto.AckTxnRequest) (*proto.CoordAck, error) {
	c.TxnMu.Lock()
	defer c.TxnMu.Unlock()

	txnId := req.TxnId

	txn, ok := c.TxnMap[txnId]
	if !ok || txn.Status != TxnCommitted {
		return &proto.CoordAck{Success: false}, nil
	}

	log.Printf("Client acknowledged committed txn %s", txnId)
	delete(c.TxnMap, txnId)

	// Remove txn from log
	data, err := os.ReadFile(c.logPath)
	if err != nil {
		log.Printf("Failed to read log file for cleanup: %v", err)
		return &proto.CoordAck{Success: false}, nil
	}

	lines := strings.Split(string(data), "\n")
	var updated []string
	for _, line := range lines {
		// Match lines like: "Transaction abc123: COMMITTED"
		if !strings.Contains(line, fmt.Sprintf("Transaction %s:", txnId)) {
			updated = append(updated, line)
		}
	}

	err = os.WriteFile(c.logPath, []byte(strings.Join(updated, "\n")), 0644)
	if err != nil {
		log.Printf("Failed to write updated log after removing txn %s: %v", txnId, err)
	} else {
		log.Printf("Txn %s removed from log after client ACK", txnId)
	}
	return &proto.CoordAck{Success: true}, nil
}

// Load transactions in progress from log file: only one txn will be prepare or pending
func (c *Coordinator) RecoverTxnLog() *Transaction {
	file, err := os.Open(c.logPath)
	if err != nil {
		log.Printf("Failed to read log: %v", err)
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	txnMap := make(map[string]*Transaction)

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		txnId := fields[1]
		status := fields[2]
		txn := txnMap[txnId]
		if txn == nil {
			txn = &Transaction{ID: txnId}
			txnMap[txnId] = txn
		}
		txn.Status = TxnStatus(status)
		log.Printf("Recovered txn %s with status %s", txnId, status)
	}

	for _, txn := range txnMap {
		if txn.Status == TxnPrepared || txn.Status == TxnPending {
			c.TxnMap[txn.ID] = txn
			return txn
		}
	}
	return nil
}

// Append to queue file instead of saving full queue
func (c *Coordinator) AppendTxnToQueueFile(txn *Transaction, queuePath string) error {
	f, err := os.OpenFile(queuePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(txn)
	if err != nil {
		return err
	}
	_, err = f.WriteString(string(data) + "\n")
	return err
}

// Recover queue by reading line-by-line appended JSON objects
func (c *Coordinator) RecoverQueueFromFile(queuePath string) error {
	file, err := os.Open(queuePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var txn Transaction
		if err := json.Unmarshal([]byte(line), &txn); err != nil {
			continue
		}
		c.TxnQueue = append(c.TxnQueue, &txn)
		c.TxnMap[txn.ID] = &txn
		log.Printf("Recovered queued txn %s", txn.ID)
	}
	return scanner.Err()
}

// Remove processed transaction from queue.json
func (c *Coordinator) RemoveTxnFromQueueFile(txnId string, queuePath string) error {
	data, err := os.ReadFile(queuePath)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	var updated []string
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var txn Transaction
		if err := json.Unmarshal([]byte(line), &txn); err != nil {
			continue
		}
		if txn.ID != txnId {
			updated = append(updated, line)
		}
	}
	return os.WriteFile(queuePath, []byte(strings.Join(updated, "\n")), 0644)
}
func (c *Coordinator) hasTxnCommittedInLog(txnId string) bool {
	data, err := os.ReadFile(c.logPath)
	if err != nil {
		return false
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "Transaction "+txnId+": COMMITTED") {
			return true
		}
	}
	return false
}
func sendAckToClient(addr, txnId, status string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to contact client %s for ACK: %v", addr, err)
		return err
	}
	defer conn.Close()

	client := proto.NewClientServiceClient(conn)
	_, err = client.ReceiveTxnStatus(context.Background(), &proto.TxnStatusUpdate{
		TxnId:  txnId,
		Status: status,
	})
	if err != nil {
		log.Printf("Failed to send ACK to client %s for txn %s: %v", addr, txnId, err)
	}
	return err
}

// func (c *Coordinator) QueueWorker() {
// 	recovered := c.RecoverTxnLog()
// 	if recovered != nil {
// 		c.ProcessTransaction(recovered)
// 	}

// 	for {
// 		c.QueueMu.Lock()
// 		if len(c.TxnQueue) == 0 {
// 			c.QueueMu.Unlock()
// 			time.Sleep(100 * time.Millisecond)
// 			continue
// 		}
// 		txn := c.TxnQueue[0]
// 		c.TxnQueue = c.TxnQueue[1:]
// 		c.QueueMu.Unlock()

// 		// Check if transaction already committed in log
// 		if c.hasTxnCommittedInLog(txn.ID) {
// 			log.Printf("Txn %s already committed. Sending commit ACK to %s", txn.ID, txn.ClientAddr)
// 			_ = sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED")
// 			continue
// 		}

// 		// Check if it's in prepare state (skip duplicate)
// 		if txn.Status == TxnPrepared {
// 			log.Printf("Txn %s is still in PREPARE. Discarding duplicate.", txn.ID)
// 			continue
// 		}

// 		err := c.ProcessTransaction(txn)

// 		if err == nil && txn.Status == TxnCommitted {
// 			log.Printf("Txn %s committed. Sending ACK to client %s", txn.ID, txn.ClientAddr)
// 			_ = sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED")
// 			// leave in log; will be cleaned by AckTxn
// 		} else {
// 			log.Printf("Txn %s aborted. Not sending ACK.", txn.ID)
// 			// optional: remove from log here if aborted
// 		}

// 		_ = c.RemoveTxnFromQueueFile(txn.ID, "queue.json")
// 	}
// }

func (c *Coordinator) QueueWorker() {
	recovered := c.RecoverTxnLog()
	if recovered != nil {
		c.ProcessTransaction(recovered)
	}

	for {
		c.QueueMu.Lock()
		if len(c.TxnQueue) == 0 {
			c.QueueMu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		txn := c.TxnQueue[0]
		c.TxnQueue = c.TxnQueue[1:]
		c.QueueMu.Unlock()

		if c.hasTxnCommittedInLog(txn.ID) {
			log.Printf("Txn %s already committed. Sending commit ACK to %s", txn.ID, txn.ClientAddr)
			_ = sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED")
			continue
		}

		if txn.Status == TxnPrepared {
			log.Printf("Txn %s is still in PREPARE. Discarding duplicate.", txn.ID)
			continue
		}

		err := c.ProcessTransaction(txn)

		if err == nil && txn.Status == TxnCommitted {
			log.Printf("Txn %s committed. Sending ACK to client %s", txn.ID, txn.ClientAddr)
			_ = sendAckToClient(txn.ClientAddr, txn.ID, "COMMITTED")
			// leave in log; will be cleaned by AckTxn
		} else {
			log.Printf("Txn %s aborted. Not sending ACK.", txn.ID)
			// optional: remove from log here if aborted
		}

		_ = c.RemoveTxnFromQueueFile(txn.ID, "queue.json")
	}
}

// Update ChannelWorker to append to queue file
func (c *Coordinator) ChannelWorker() {
	for txn := range c.ProcessChannel {
		c.QueueMu.Lock()
		c.TxnQueue = append(c.TxnQueue, txn)
		_ = c.AppendTxnToQueueFile(txn, "queue.json")
		c.QueueMu.Unlock()

		c.TxnMu.Lock()
		c.TxnMap[txn.ID] = txn
		c.TxnMu.Unlock()

		log.Printf("Added transaction %s to the queue", txn.ID)
	}
}

// Enhance NewCoordinator to include queue + txn log recovery
func NewCoordinator(BinsJSON string, logPath string, timeout time.Duration, maxNumTransactions int) (*Coordinator, error) {
	coordinator := &Coordinator{
		TxnMap:         make(map[string]*Transaction),
		Timeout:        timeout,
		ProcessChannel: make(chan *Transaction, maxNumTransactions),
		logPath:        logPath,
	}

	if err := coordinator.LoadBinMappingConfig(BinsJSON); err != nil {
		return nil, fmt.Errorf("could not load %s", BinsJSON)
	}

	_ = coordinator.RecoverQueueFromFile("queue.json")

	go coordinator.ChannelWorker()
	go coordinator.QueueWorker()
	return coordinator, nil
}

func (c *Coordinator) GetStatus(ctx context.Context, req *proto.GetStatusRequest) (*proto.GetStatusReply, error) {
	c.TxnMu.Lock()
	defer c.TxnMu.Unlock()

	txn, ok := c.TxnMap[req.TxnId]
	if ok {
		return &proto.GetStatusReply{Status: string(txn.Status)}, nil
	}

	// Optionally: parse c.logPath and scan for committed/aborted txn
	// Otherwise, return UNKNOWN
	return &proto.GetStatusReply{Status: "UNKNOWN"}, nil
}

func (c *Coordinator) LoadBinMappingConfig(path string) error {
	file, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var config struct {
		BinMap  map[string]string `json:"bin_map"`
		NumBins int               `json:"num_bins"`
	}

	if err := json.Unmarshal(file, &config); err != nil {
		return err
	}

	c.BinsToBackend = config.BinMap
	c.NumBins = config.NumBins
	return nil
}

func (c *Coordinator) LogToFile(entry string) error {
	c.logMu.Lock()
	defer c.logMu.Unlock()

	file, err := os.OpenFile(c.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(entry)
	return err
}

// Updated ProcessTransaction to support different restart stages with timeout for prepare
func (c *Coordinator) ProcessTransaction(txn *Transaction) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	if txn.Status == TxnPrepared {
		log.Printf("Restarting txn %s from prepared — starting commit phase", txn.ID)
		if !c.CommitPhase(ctx, txn) {
			txn.Status = TxnAborted
			c.AbortPhase(ctx, txn)
			logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
			return c.LogToFile(logEntry)
		}
		logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnCommitted)
		c.LogToFile(logEntry)
		txn.Status = TxnCommitted
		return nil
	} else if txn.Status == TxnPending {
		log.Printf("Restarting txn %s from pending — redoing prepare phase", txn.ID)
	} else {
		logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnPending)
		err := c.LogToFile(logEntry)
		if err != nil {
			txn.Status = TxnAborted
			logEntry = fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
			c.LogToFile(logEntry)
			return err
		}
		txn.Status = TxnPending
	}

	done := make(chan bool, 1)
	go func() {
		done <- c.PreparePhase(ctx, txn)
	}()

	select {
	case success := <-done:
		if !success {
			txn.Status = TxnAborted
			c.AbortPhase(ctx, txn)
			logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
			c.LogToFile(logEntry)
			return nil
		}
	case <-ctx.Done():
		log.Printf("Timeout waiting for prepare acks on txn %s — aborting", txn.ID)
		txn.Status = TxnAborted
		c.AbortPhase(ctx, txn)
		logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
		c.LogToFile(logEntry)
		return nil
	}

	logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnPrepared)
	c.LogToFile(logEntry)
	txn.Status = TxnPrepared

	if !c.CommitPhase(ctx, txn) {
		txn.Status = TxnAborted
		c.AbortPhase(ctx, txn)
		logEntry = fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnAborted)
		c.LogToFile(logEntry)
		return nil
	}
	logEntry = fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnCommitted)
	c.LogToFile(logEntry)
	txn.Status = TxnCommitted

	return nil
}

func (c *Coordinator) PreparePhase(ctx context.Context, txn *Transaction) bool {
	backends := c.GetAssociatedBackendsPrepare(txn)

	prepareCh := make(chan bool, len(backends))

	for backendAddr, prepareReq := range backends {
		go func(addr string, req *proto.PrepareRequest) {
			success := c.sendPrepareToBackends(ctx, addr, req)
			prepareCh <- success
		}(backendAddr, prepareReq)
	}

	successCount := 0
	totalBackends := len(backends)

	for i := 0; i < totalBackends; i++ {
		select {
		case success := <-prepareCh:
			if success {
				successCount++
			} else {
				log.Printf("Prepare failed for transaction %s", txn.ID)
				return false
			}
		case <-ctx.Done():
			log.Printf("Prepare phase timed out for transaction %s", txn.ID)
			return false
		}
	}

	allSuccess := successCount == totalBackends
	if allSuccess {
		log.Printf("Prepare phase successful for transaction %s", txn.ID)
	} else {
		log.Printf("Prepare phase failed for transaction %s (%d/%d backends succeeded)",
			txn.ID, successCount, totalBackends)
	}

	return allSuccess
}

// func (c *Coordinator) GetAssociatedBackendsPrepare(txn *Transaction) map[string]*proto.PrepareRequest {
// 	backends := make(map[string]*proto.PrepareRequest)

// 	fromBin := c.hashToBin(txn.From)
// 	fromBackend := c.BinsToBackend[fromBin]
// 	fromKey := fmt.Sprintf("%s::%s", fromBin, txn.From)

// 	toBin := c.hashToBin(txn.To)
// 	toBackend := c.BinsToBackend[toBin]
// 	toKey := fmt.Sprintf("%s::%s", toBin, txn.To)

// 	backends[fromBackend] = &proto.PrepareRequest{
// 		TxnId:     txn.ID,
// 		Key:       fromKey,
// 		Value:     fmt.Sprintf("%d", txn.Amount),
// 		Operation: "DEBIT",
// 	}

// 	backends[toBackend] = &proto.PrepareRequest{
// 		TxnId:     txn.ID,
// 		Key:       toKey,
// 		Value:     fmt.Sprintf("%d", txn.Amount),
// 		Operation: "CREDIT",
// 	}

// 	return backends
// }

func (c *Coordinator) GetAssociatedBackendsPrepare(txn *Transaction) map[string]*proto.PrepareRequest {
	backends := make(map[string]*proto.PrepareRequest)

	if txn.From != "" {
		fromBin := c.hashToBin(txn.From)
		fromBackend := c.BinsToBackend[fromBin]
		fromKey := fmt.Sprintf("%s::%s", fromBin, txn.From)

		backends[fromBackend] = &proto.PrepareRequest{
			TxnId:     txn.ID,
			Key:       fromKey,
			Value:     fmt.Sprintf("%d", txn.Amount),
			Operation: "DEBIT",
		}
	}

	if txn.To != "" {
		toBin := c.hashToBin(txn.To)
		toBackend := c.BinsToBackend[toBin]
		toKey := fmt.Sprintf("%s::%s", toBin, txn.To)

		backends[toBackend] = &proto.PrepareRequest{
			TxnId:     txn.ID,
			Key:       toKey,
			Value:     fmt.Sprintf("%d", txn.Amount),
			Operation: "CREDIT",
		}
	}

	return backends
}

func (c *Coordinator) sendPrepareToBackends(ctx context.Context, backendAddr string, req *proto.PrepareRequest) bool {
	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to backend %s: %v", backendAddr, err)
		return false
	}
	defer conn.Close()

	client := proto.NewKVServiceClient(conn)

	resp, err := client.Prepare(ctx, req)
	if err != nil {
		log.Printf("Prepare failed for backend %s, key %s: %v", backendAddr, req.Key, err)
		return false
	}

	if !resp.Success {
		log.Printf("Prepare rejected by backend %s for key %s", backendAddr, req.Key)
		return false
	}

	log.Printf("Prepare successful for backend %s, key %s, operation %s", backendAddr, req.Key, req.Operation)
	return true
}

func (c *Coordinator) CommitPhase(ctx context.Context, txn *Transaction) bool {
	backends := c.GetAssociatedBackendsCommit(txn)

	commitCh := make(chan bool, len(backends))

	for backendAddr, commitReq := range backends {
		go func(addr string, req *proto.CommitRequest) {
			success := c.sendCommitToBackends(ctx, addr, req)
			commitCh <- success
		}(backendAddr, commitReq)
	}

	successCount := 0
	totalBackends := len(backends)

	for i := 0; i < totalBackends; i++ {
		select {
		case success := <-commitCh:
			if success {
				successCount++
			} else {
				log.Printf("Commit failed for transaction %s", txn.ID)
				return false
			}
		case <-ctx.Done():
			log.Printf("Commit phase timed out for transaction %s", txn.ID)
			return false
		}
	}

	allSuccess := successCount == totalBackends
	if allSuccess {
		log.Printf("Commit phase successful for transaction %s", txn.ID)
	} else {
		log.Printf("Commit phase failed for transaction %s (%d/%d backends succeeded)",
			txn.ID, successCount, totalBackends)
	}

	return allSuccess
}

func (c *Coordinator) GetAssociatedBackendsCommit(txn *Transaction) map[string]*proto.CommitRequest {
	backends := make(map[string]*proto.CommitRequest)

	fromBin := c.hashToBin(txn.From)
	fromBackend := c.BinsToBackend[fromBin]

	toBin := c.hashToBin(txn.To)
	toBackend := c.BinsToBackend[toBin]

	backends[fromBackend] = &proto.CommitRequest{
		TxnId: txn.ID,
	}

	backends[toBackend] = &proto.CommitRequest{
		TxnId: txn.ID,
	}

	return backends
}

func (c *Coordinator) sendCommitToBackends(ctx context.Context, backendAddr string, req *proto.CommitRequest) bool {
	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to backend %s: %v", backendAddr, err)
		return false
	}
	defer conn.Close()

	client := proto.NewKVServiceClient(conn)

	resp, err := client.Commit(ctx, req)
	if err != nil {
		log.Printf("Commit failed for backend %s, txn %s: %v", backendAddr, req.TxnId, err)
		return false
	}

	if !resp.Success {
		log.Printf("Commit rejected by backend %s for txn %s", backendAddr, req.TxnId)
		return false
	}

	log.Printf("Commit successful for backend %s, txn %s", backendAddr, req.TxnId)
	return true
}

func (c *Coordinator) AbortPhase(ctx context.Context, txn *Transaction) {
	backends := c.GetAssociatedBackendsAbort(txn)

	abortCh := make(chan bool, len(backends))

	for backendAddr, abortReq := range backends {
		go func(addr string, req *proto.AbortRequest) {
			success := c.sendAbortToBackends(ctx, addr, req)
			abortCh <- success
		}(backendAddr, abortReq)
	}

	totalBackends := len(backends)
	successCount := 0

	for i := 0; i < totalBackends; i++ {
		select {
		case success := <-abortCh:
			if success {
				successCount++
			}
		case <-ctx.Done():
			log.Printf("Abort phase timed out for transaction %s", txn.ID)
			return
		}
	}

	log.Printf("Abort phase completed for transaction %s (%d/%d backends responded)",
		txn.ID, successCount, totalBackends)
}

func (c *Coordinator) GetAssociatedBackendsAbort(txn *Transaction) map[string]*proto.AbortRequest {
	backends := make(map[string]*proto.AbortRequest)

	fromBin := c.hashToBin(txn.From)
	fromBackend := c.BinsToBackend[fromBin]

	toBin := c.hashToBin(txn.To)
	toBackend := c.BinsToBackend[toBin]

	backends[fromBackend] = &proto.AbortRequest{
		TxnId: txn.ID,
	}

	backends[toBackend] = &proto.AbortRequest{
		TxnId: txn.ID,
	}

	return backends
}

func (c *Coordinator) sendAbortToBackends(ctx context.Context, backendAddr string, req *proto.AbortRequest) bool {
	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to backend %s for abort: %v", backendAddr, err)
		return false
	}
	defer conn.Close()

	client := proto.NewKVServiceClient(conn)

	resp, err := client.Abort(ctx, req)
	if err != nil {
		log.Printf("Abort failed for backend %s, txn %s: %v", backendAddr, req.TxnId, err)
		return false
	}

	if !resp.Success {
		log.Printf("Abort rejected by backend %s for txn %s", backendAddr, req.TxnId)
		return false
	}

	log.Printf("Abort successful for backend %s, txn %s", backendAddr, req.TxnId)
	return true
}

func (c *Coordinator) hashToBin(account string) string {
	h := fnv.New32a()
	h.Write([]byte(account))
	binIndex := int(h.Sum32()) % c.NumBins
	return fmt.Sprintf("bin%d", binIndex)
}
