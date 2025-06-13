package server

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

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
				txnId := fields[1]
				delete(s.prepare_log, txnId)
			} else {
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
