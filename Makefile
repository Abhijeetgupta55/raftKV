# Build entry points for raftkv.
#
# Recipes assume a POSIX shell; on Windows, run make from Git Bash (sh.exe
# must be on PATH). `make proto` additionally needs protoc plus the
# protoc-gen-go and protoc-gen-go-grpc plugins — regular builds don't,
# because generated code is committed (see docs/DECISIONS/0001).

BIN := bin
ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: all build test race vet fmt fmt-check proto run-server clean

all: build

build:
	go build -o $(BIN)/kvserver$(EXE) ./cmd/server
	go build -o $(BIN)/kvcli$(EXE) ./cmd/cli

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "files need gofmt:"; echo "$$out"; exit 1; fi

proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/kv/v1/kv.proto proto/raft/v1/raft.proto

run-server: build
	$(BIN)/kvserver$(EXE)

clean:
	rm -rf $(BIN)
