package coordinator

import (
	"context"
	"log"

	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func sendAckToClient(addr, txnId, status string) error {
	log.Printf("sendAckToClient: Sending ACK to client %s for transaction %s with status %s", addr, txnId, status)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sendAckToClient: Failed to establish connection to client %s: %v", addr, err)
		return err
	}
	defer conn.Close()
	log.Printf("sendAckToClient: Successfully established connection to client %s", addr)

	client := proto.NewClientServiceClient(conn)
	_, err = client.ReceiveTxnStatus(context.Background(), &proto.TxnStatusUpdate{
		TxnId:  txnId,
		Status: status,
	})

	if err != nil {
		log.Printf("sendAckToClient: Failed to send ACK to client %s for transaction %s: %v", addr, txnId, err)
	} else {
		log.Printf("sendAckToClient: Successfully sent ACK to client %s for transaction %s", addr, txnId)
	}

	return err
}
