package coordinator

import (
	"context"
	"fmt"
	"log"

	"github.com/harshitster/223B-Project/code/proto"
)

type BatchPrepareRequest struct {
	TxnId      string
	Operations []*proto.PrepareRequest
}

func (c *Coordinator) PreparePhase(ctx context.Context, txn *Transaction) bool {
	log.Printf("PreparePhase: Starting prepare phase for transaction %s", txn.ID)

	backends := c.GetAssociatedBackendsPrepare(txn)
	totalBackends := len(backends)
	log.Printf("PreparePhase: Found %d backends to prepare for transaction %s", totalBackends, txn.ID)

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

	for backendAddr, batchReq := range backends {
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
