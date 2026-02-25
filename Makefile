BIN_DIR := bin

.PHONY: serve-easy serve-medium-server-1 serve-medium-server-2 test build-easy-server build-medium-server-1 build-medium-server-2 build-compare-csv build-process-products build-shuffle-csv build-all clean-binaries

serve-easy:
	GOWORK=off go run ./cmd/easy-server -path outputs/sample_products_cleaned.sqlite -id gtin

serve-medium-server-1:
	GOWORK=off go run ./cmd/medium-server-1 -path outputs/sample_products_cleaned.sqlite -id gtin

serve-medium-server-2:
	GOWORK=off go run ./cmd/medium-server-2 -path outputs/sample_products_cleaned.sqlite -id gtin

test:
	GOCACHE=/tmp/gocache GOWORK=off go test ./cmd/compare-csv -v

build-easy-server:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/easy-server ./cmd/easy-server

build-medium-server-1:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/medium-server-1 ./cmd/medium-server-1

build-medium-server-2:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/medium-server-2 ./cmd/medium-server-2

build-compare-csv:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/compare-csv ./cmd/compare-csv

build-process-products:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/process-products ./cmd/process-products

build-shuffle-csv:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/shuffle-csv ./cmd/shuffle-csv

build-all: build-easy-server build-medium-server-1 build-medium-server-2 build-compare-csv build-process-products build-shuffle-csv

clean-binaries:
	rm -f easy-server medium-server medium-server-1 medium-server-2 compare-csv process-products shuffle-csv
