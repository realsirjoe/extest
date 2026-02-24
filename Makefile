.PHONY: serve-easy

serve-easy:
	GOWORK=off go run ./cmd/easy-server -path outputs/dm_products_cleaned.sqlite -id gtin
