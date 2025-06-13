package server

import (
	"log"
	"sync"

	"github.com/harshitster/223B-Project/code/proto"
)

type KVServer struct {
	proto.UnimplementedKVServiceServer

	store              map[string]int
	prepare_log        map[string]map[string]PreparedTxn
	mu                 sync.Mutex
	logFilePath        string
	peerAddresses      []string
	selfAddress        string
	coordinatorAddress string
}

type PreparedTxn struct {
	key   string
	value int
}

func NewKVServer(logFilePath string, peerAddresses []string, selfAddress string, coordinatorAddress string) *KVServer {
	s := &KVServer{
		store:              make(map[string]int),
		prepare_log:        make(map[string]map[string]PreparedTxn),
		logFilePath:        logFilePath,
		peerAddresses:      peerAddresses,
		selfAddress:        selfAddress,
		coordinatorAddress: coordinatorAddress,
	}
	if err := s.RecoverFromLog(); err != nil {
		log.Printf("Warning: failed to recover from log: %v", err)
	} else {
		log.Printf("Recovery from log completed.")
	}

	return s
}
