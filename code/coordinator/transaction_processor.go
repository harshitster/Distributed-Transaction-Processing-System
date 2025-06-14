package coordinator

import (
	"context"
	"fmt"
	"log"
)

func (c *Coordinator) ProcessTransaction(txn *Transaction) error {
	log.Printf("ProcessTransaction: Starting to process transaction %s with current status: %v", txn.ID, txn.Status)

	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	log.Printf("ProcessTransaction: Created context with timeout %v for transaction %s", c.Timeout, txn.ID)

	if txn.Status == TxnPrepared {
		log.Printf("ProcessTransaction: Transaction %s is in PREPARED state, restarting from commit phase", txn.ID)

		if !c.CommitPhase(ctx, txn) {
			log.Printf("ProcessTransaction: Commit phase failed for transaction %s, will rely on PostPrepare — NOT aborting", txn.ID)
			// return nil
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
	logEntry := fmt.Sprintf("Transaction %s: %s\n", txn.ID, TxnPrepared)
	if logErr := c.LogToFile(logEntry); logErr != nil {
		log.Printf("ProcessTransaction: Failed to log prepared state for transaction %s: %v", txn.ID, logErr)
	}
	txn.Status = TxnPrepared

	log.Printf("ProcessTransaction: Starting commit phase for transaction %s", txn.ID)
	if !c.CommitPhase(ctx, txn) {
		log.Printf("ProcessTransaction: Commit phase failed for transaction %s, will rely on PostPrepare — NOT aborting", txn.ID)
		// return nil
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
