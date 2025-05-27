package coordinator

import (
	"context"
	"sync"
	"time"
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

func NewCoordinator(logPath string, timeout time.Duration) (*Coordinator, error) {
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

// sendPrepareToBackends is a mock of network RPC
func sendPrepareToBackends(ctx context.Context, txn *Transaction) bool {
	// placeholder logic
	select {
	case <-ctx.Done():
		return false
	case <-time.After(300 * time.Millisecond):
		return true
	}
}
