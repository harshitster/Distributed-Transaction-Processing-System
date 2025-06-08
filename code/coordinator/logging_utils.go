package coordinator

import (
	"bufio"
	"log"
	"os"
	"strings"
)

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
