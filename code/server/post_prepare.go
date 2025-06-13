package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *KVServer) PostPrepare(txnId string) {
	time.Sleep(10 * time.Second)

	log.Printf("PostPrepare: Checking local log file %s for transaction %s", s.logFilePath, txnId)
	data, err := os.ReadFile(s.logFilePath)
	if err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.Contains(line, fmt.Sprintf("COMMIT %s", txnId)) {
				log.Printf("PostPrepare: Found COMMIT entry in local log for txn %s, exiting", txnId)
				return
			}
		}
	} else {
		log.Printf("PostPrepare: Failed to read log file %s: %v", s.logFilePath, err)
	}

	status := s.queryCoordinator(txnId)
	if status == "COMMITTED" {
		log.Printf("Coordinator says COMMITTED for txn %s", txnId)
		s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
		return
	} else if status == "ABORTED" {
		log.Printf("Coordinator says ABORTED for txn %s", txnId)
		s.Abort(context.Background(), &proto.AbortRequest{TxnId: txnId})
		return
	}

	result := s.queryBackendsForTxnStatus(txnId)
	if result == "COMMITTED" {
		log.Printf("Peer confirms COMMITTED for txn %s", txnId)
		s.Commit(context.Background(), &proto.CommitRequest{TxnId: txnId})
	} else if result == "UNKNOWN" {
		log.Printf("Txn %s discarded after timeout", txnId)
		s.mu.Lock()
		txnOps, exists := s.prepare_log[txnId]
		if exists {
			for key, prep := range txnOps {
				discardEntry := fmt.Sprintf("DISCARD %s %s %d\n", txnId, key, prep.value)
				s.logToFile(discardEntry)
			}
			delete(s.prepare_log, txnId)
		}
		s.mu.Unlock()
	}
}

func (s *KVServer) QueryTxnStatus(ctx context.Context, req *proto.QueryTxnRequest) (*proto.QueryTxnReply, error) {
	txnId := req.TxnId

	s.mu.Lock()
	_, prepared := s.prepare_log[txnId]
	s.mu.Unlock()
	if prepared {
		return &proto.QueryTxnReply{Status: "PREPARED"}, nil
	}

	data, err := os.ReadFile(s.logFilePath)
	if err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "COMMIT "+txnId) {
				return &proto.QueryTxnReply{Status: "COMMITTED"}, nil
			}
			if strings.HasPrefix(line, "DISCARD "+txnId) {
				return &proto.QueryTxnReply{Status: "DISCARDED"}, nil
			}
			if strings.HasPrefix(line, "ABORT "+txnId) {
				return &proto.QueryTxnReply{Status: "ABORTED"}, nil
			}
		}
	}

	return &proto.QueryTxnReply{Status: "UNKNOWN"}, nil
}

func (s *KVServer) queryBackendsForTxnStatus(txnId string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Initiated Query Backends for txn id: %s", txnId)

	resultCh := make(chan string, len(s.peerAddresses))

	for _, addr := range s.peerAddresses {
		if addr == s.selfAddress {
			continue
		}

		go func(peerAddr string) {
			conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				resultCh <- "UNKNOWN"
				return
			}
			defer conn.Close()

			client := proto.NewKVServiceClient(conn)
			resp, err := client.QueryTxnStatus(ctx, &proto.QueryTxnRequest{TxnId: txnId})
			if err != nil {
				resultCh <- "UNKNOWN"
				return
			}
			resultCh <- resp.Status
		}(addr)
	}

	expected := len(s.peerAddresses) - 1
	received := 0
	anyCommitted := false

	for {
		select {
		case status := <-resultCh:
			received++
			if status == "COMMITTED" {
				anyCommitted = true
			}
			if received == expected {
				if anyCommitted {
					return "COMMITTED"
				}
				return "UNKNOWN"
			}
		case <-ctx.Done():
			log.Printf("Timeout while querying peers for txn %s", txnId)
			if anyCommitted {
				return "COMMITTED"
			}
			return "UNKNOWN"
		}
	}
}

func (s *KVServer) queryCoordinator(txnId string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Initiated Query Coordinator for txn id: %s", txnId)

	conn, err := grpc.Dial(s.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to coordinator: %v", err)
		return "UNKNOWN"
	}
	defer conn.Close()

	client := proto.NewCoordinatorServiceClient(conn)

	respCh := make(chan string, 1)

	go func() {
		resp, err := client.GetStatus(ctx, &proto.GetStatusRequest{TxnId: txnId})
		if err != nil {
			respCh <- "UNKNOWN"
			return
		}
		respCh <- resp.Status
	}()

	select {
	case <-ctx.Done():
		log.Printf("Timeout while querying coordinator for txn %s", txnId)
		return "UNKNOWN"
	case status := <-respCh:
		return status
	}
}
