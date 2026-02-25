.PHONY: serve-easy test

serve-easy:
	GOWORK=off go run ./cmd/easy-server -path outputs/dm_products_cleaned.sqlite -id gtin

test:
	GOCACHE=/tmp/gocache GOWORK=off go test ./cmd/compare-csv -v
