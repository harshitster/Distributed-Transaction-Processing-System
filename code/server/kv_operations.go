package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/harshitster/223B-Project/code/proto"
)

func (s *KVServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.ValueReply, error) {
	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.store[req.Key]
	if !exists {
		return &proto.ValueReply{Value: "0"}, nil
	}

	return &proto.ValueReply{Value: strconv.Itoa(value)}, nil
}

func (s *KVServer) Set(ctx context.Context, req *proto.SetRequest) (*proto.Ack, error) {
	if req.Key == "" {
		return &proto.Ack{Success: false}, fmt.Errorf("key cannot be empty")
	}

	value, err := strconv.Atoi(req.Value)
	if err != nil {
		return &proto.Ack{Success: false}, fmt.Errorf("invalid value: must be a number")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[req.Key] = value
	return &proto.Ack{Success: true}, nil
}
