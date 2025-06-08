package coordinator

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"
)

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
