.PHONY: all
all: proto
.PHONY: proto
proto:
	protoc --go_out=. --go-grpc_out=. code/proto/*.proto
