SHELL := /bin/sh

APP := databasa
CMD := ./cmd/databasa-server
BIN_DIR := ./bin
BIN := $(BIN_DIR)/$(APP)
DATA_DIR := ./data

.PHONY: all proto tidy build run test clean docker-build docker-run

all: test build

proto:
	protoc -I proto \
		--go_out=. --go_opt=module=github.com/futlize/databasa \
		--go-grpc_out=. --go-grpc_opt=module=github.com/futlize/databasa \
		proto/databasa.proto

tidy:
	go mod tidy

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN) $(CMD)

run:
	go run $(CMD) -config ./databasa.toml

test:
	go test ./...

clean:
	rm -rf $(BIN_DIR) $(DATA_DIR)

docker-build:
	docker build -t databasa:latest .

docker-run:
	docker run --rm -p 50051:50051 -v $(PWD)/data:/app/data databasa:latest
