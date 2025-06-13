package coordinator

import (
	"context"
	"log"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (c *Coordinator) sendPrepareToBackends(ctx context.Context, backendAddr string, req *proto.PrepareRequest) bool {
	log.Printf("sendPrepareToBackends: Sending prepare request to backend %s for transaction %s", backendAddr, req.TxnId)
	log.Printf("sendPrepareToBackends: Request details - Key: %s, Value: %s, Operation: %s",
		req.Key, req.Value, req.Operation)

	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendPrepareToBackends: Failed to establish connection to backend %s for transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	defer conn.Close()
	log.Printf("sendPrepareToBackends: Successfully connected to backend %s", backendAddr)

	client := proto.NewKVServiceClient(conn)
	log.Printf("sendPrepareToBackends: Created KV service client for backend %s", backendAddr)

	resp, err := client.Prepare(ctx, req)
	if err != nil {
		log.Printf("sendPrepareToBackends: gRPC call failed for backend %s, key %s, transaction %s: %v",
			backendAddr, req.Key, req.TxnId, err)
		return false
	}
	log.Printf("sendPrepareToBackends: Received response from backend %s for transaction %s", backendAddr, req.TxnId)

	if !resp.Success {
		log.Printf("sendPrepareToBackends: Prepare rejected by backend %s for key %s, transaction %s (Success=false)",
			backendAddr, req.Key, req.TxnId)
		return false
	}

	log.Printf("sendPrepareToBackends: Prepare successful for backend %s, key %s, operation %s, transaction %s",
		backendAddr, req.Key, req.Operation, req.TxnId)
	return true
}

func (c *Coordinator) sendCommitToBackends(ctx context.Context, backendAddr string, req *proto.CommitRequest) bool {
	log.Printf("sendCommitToBackends: Sending commit request to backend %s for transaction %s", backendAddr, req.TxnId)

	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendCommitToBackends: Failed to establish connection to backend %s for transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	defer conn.Close()
	log.Printf("sendCommitToBackends: Successfully connected to backend %s", backendAddr)

	client := proto.NewKVServiceClient(conn)
	log.Printf("sendCommitToBackends: Created KV service client for backend %s", backendAddr)

	resp, err := client.Commit(ctx, req)
	if err != nil {
		log.Printf("sendCommitToBackends: gRPC call failed for backend %s, transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	log.Printf("sendCommitToBackends: Received response from backend %s for transaction %s", backendAddr, req.TxnId)

	if !resp.Success {
		log.Printf("sendCommitToBackends: Commit rejected by backend %s for transaction %s (Success=false)",
			backendAddr, req.TxnId)
		return false
	}

	log.Printf("sendCommitToBackends: Commit successful for backend %s, transaction %s", backendAddr, req.TxnId)
	return true
}

func (c *Coordinator) sendAbortToBackends(ctx context.Context, backendAddr string, req *proto.AbortRequest) bool {
	log.Printf("sendAbortToBackends: Sending abort request to backend %s for transaction %s", backendAddr, req.TxnId)

	conn, err := grpc.Dial(backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendAbortToBackends: Failed to establish connection to backend %s for transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	defer conn.Close()
	log.Printf("sendAbortToBackends: Successfully connected to backend %s", backendAddr)

	client := proto.NewKVServiceClient(conn)
	log.Printf("sendAbortToBackends: Created KV service client for backend %s", backendAddr)

	resp, err := client.Abort(ctx, req)
	if err != nil {
		log.Printf("sendAbortToBackends: gRPC call failed for backend %s, transaction %s: %v",
			backendAddr, req.TxnId, err)
		return false
	}
	log.Printf("sendAbortToBackends: Received response from backend %s for transaction %s", backendAddr, req.TxnId)

	if !resp.Success {
		log.Printf("sendAbortToBackends: Abort rejected by backend %s for transaction %s (Success=false)",
			backendAddr, req.TxnId)
		return false
	}

	log.Printf("sendAbortToBackends: Abort successful for backend %s, transaction %s", backendAddr, req.TxnId)
	return true
}
