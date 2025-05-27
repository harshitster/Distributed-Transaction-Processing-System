package coordinator

import (
	"encoding/json"
	"os"
	"sync"
)

type LogManager struct {
	mu   sync.Mutex
	file *os.File
}

func NewLogManager(path string) (*LogManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &LogManager{file: f}, nil
}

func (l *LogManager) Append(txn *Transaction) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	data, err := json.Marshal(txn)
	if err != nil {
		return err
	}
	_, err = l.file.Write(append(data, '\n'))
	return err
}
