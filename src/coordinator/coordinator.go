package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	kvproto "github.com/harshitster/223B-Project/src/coordinator/proto"
)

type Coordinator struct {
	logs      *LogManager
	queue     []*Transaction
	txnMap    map[string]*Transaction
	queueMu   sync.Mutex
	txnMu     sync.Mutex
	timeout   time.Duration
	processCh chan *Transaction
}

var binToBackend map[string]string
var numBins int

func NewCoordinator(logPath string, timeout time.Duration) (*Coordinator, error) {
	_ = LoadBinMappingConfig("bins.json")
	logs, err := NewLogManager(logPath)
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		logs:      logs,
		txnMap:    make(map[string]*Transaction),
		timeout:   timeout,
		processCh: make(chan *Transaction, 100),
	}
	go c.worker()
	return c, nil
}

func (c *Coordinator) SubmitTransaction(txn *Transaction) error {
	c.queueMu.Lock()
	c.queue = append(c.queue, txn)
	c.queueMu.Unlock()

	c.txnMu.Lock()
	c.txnMap[txn.ID] = txn
	c.txnMu.Unlock()

	return c.logs.Append(txn)
}

func (c *Coordinator) worker() {
	for {
		if len(c.queue) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c.queueMu.Lock()
		txn := c.queue[0]
		c.queue = c.queue[1:]
		c.queueMu.Unlock()

		go c.processTransaction(txn)
	}
}

func (c *Coordinator) processTransaction(txn *Transaction) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Send prepare to all backends (abstracted)
	success := sendPrepareToBackends(ctx, txn)

	if success {
		txn.Status = TxnCommitted
	} else {
		txn.Status = TxnAborted
	}

	_ = c.logs.Append(txn) // log commit or abort
	// Cleanup
	c.txnMu.Lock()
	delete(c.txnMap, txn.ID)
	c.txnMu.Unlock()
}

// // sendPrepareToBackends is a mock of network RPC
// func sendPrepareToBackends(ctx context.Context, txn *Transaction) bool {
// 	// placeholder logic
// 	select {
// 	case <-ctx.Done():
// 		return false
// 	case <-time.After(300 * time.Millisecond):
// 		return true
// 	}
// }

func hashToBin(key string) string {
	h := fnv.New32a()
	h.Write([]byte(key))
	binIndex := int(h.Sum32()) % numBins
	return fmt.Sprintf("bin%d", binIndex)
}

func LoadBinMappingConfig(path string) error {
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
	binToBackend = config.BinMap
	numBins = config.NumBins
	return nil
}

func sendPrepareToBackends(ctx context.Context, txn *Transaction) bool {
	key := txn.Accounts[0] // assuming 1 key for now
	bin := hashToBin(key)
	addr := binToBackend[bin]

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("failed to connect: %v", err)
		return false
	}
	defer conn.Close()

	client := kvproto.NewKVServerClient(conn)

	// Phase 1: PREPARE
	prepResp, err := client.Prepare(ctx, &kvproto.PrepareRequest{
		TxnId:    txn.ID,
		Key:      key,
		NewValue: fmt.Sprintf("%d", txn.Amount),
	})
	if err != nil || !prepResp.Success {
		client.Abort(ctx, &kvproto.AbortRequest{TxnId: txn.ID})
		return false
	}

	// Phase 2: COMMIT
	_, err = client.Commit(ctx, &kvproto.CommitRequest{TxnId: txn.ID})
	return err == nil
}
