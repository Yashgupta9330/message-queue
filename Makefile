.PHONY: proto tidy build test build-all run scenario1 scenario2 scenario3 scenario4 scenario5 scenarios

proto:
	protoc -I proto \
	  --go_out=proto/gen --go_opt=paths=source_relative \
	  --go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
	  proto/broker.proto

tidy:
	go mod tidy

build:
	go build ./...

build-all:
	@mkdir -p bin
	go build -o bin/broker    ./cmd/broker
	go build -o bin/consumer  ./cmd/consumer
	go build -o bin/publisher ./cmd/publisher

test:
	go test ./... -race

run: build-all
	./scripts/start_server.sh

scenario1: build-all
	./scripts/scenario1.sh

scenario2: build-all
	./scripts/scenario2.sh

scenario3: build-all
	./scripts/scenario3.sh

scenario4: build-all
	./scripts/scenario4.sh

scenario5: build-all
	./scripts/scenario5.sh

scenarios: build-all
	./scripts/run_all.sh
