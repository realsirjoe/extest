BIN_DIR := bin

.PHONY: serve-easy test build-easy-server build-compare-csv build-process-products build-shuffle-csv build-all

serve-easy:
	GOWORK=off go run ./cmd/easy-server -path outputs/sample_products_cleaned.sqlite -id gtin

test:
	GOCACHE=/tmp/gocache GOWORK=off go test ./cmd/compare-csv -v

build-easy-server:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/easy-server ./cmd/easy-server

build-compare-csv:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/compare-csv ./cmd/compare-csv

build-process-products:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/process-products ./cmd/process-products

build-shuffle-csv:
	mkdir -p $(BIN_DIR)
	GOCACHE=/tmp/gocache GOWORK=off go build -o $(BIN_DIR)/shuffle-csv ./cmd/shuffle-csv

build-all: build-easy-server build-compare-csv build-process-products build-shuffle-csv
