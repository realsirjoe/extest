package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultInput  = "outputs/sample_products_reference.csv"
	defaultOutput = "outputs/sample_products_candidate1.csv"
	defaultSeed   = int64(20260224)
)

func main() {
	inPath := flag.String("input", defaultInput, "Input CSV path")
	outPath := flag.String("output", defaultOutput, "Output CSV path")
	seed := flag.Int64("seed", defaultSeed, "Deterministic shuffle seed")
	sampleRows := flag.Int("sample-rows", 0, "If > 0, keep only this many rows after shuffling")
	flag.Parse()

	headers, rows, err := loadCSV(*inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load csv error: %v\n", err)
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(*seed))
	shuffledCols := append([]string(nil), headers...)
	rng.Shuffle(len(shuffledCols), func(i, j int) { shuffledCols[i], shuffledCols[j] = shuffledCols[j], shuffledCols[i] })

	shuffledRows := append([]map[string]string(nil), rows...)
	rng.Shuffle(len(shuffledRows), func(i, j int) { shuffledRows[i], shuffledRows[j] = shuffledRows[j], shuffledRows[i] })
	if *sampleRows > 0 && *sampleRows < len(shuffledRows) {
		shuffledRows = shuffledRows[:*sampleRows]
	}

	renamedCols, renameMap := buildUniqueNames(shuffledCols)
	if err := writeCSV(*outPath, renamedCols, shuffledCols, shuffledRows, renameMap); err != nil {
		fmt.Fprintf(os.Stderr, "write csv error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Input:  %s\n", *inPath)
	fmt.Printf("Output: %s\n", *outPath)
	fmt.Printf("Seed:   %d\n", *seed)
	fmt.Printf("Rows:   %d\n", len(shuffledRows))
	fmt.Printf("Cols:   %d\n", len(shuffledCols))
	fmt.Println("Sample column mapping (first 10 in output order):")
	for i := 0; i < len(shuffledCols) && i < 10; i++ {
		c := shuffledCols[i]
		fmt.Printf("  %s -> %s\n", c, renameMap[c])
	}
}

func loadCSV(path string) ([]string, []map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF})
	r := csv.NewReader(bytes.NewReader(b))
	r.FieldsPerRecord = -1
	headers, err := r.Read()
	if err != nil {
		return nil, nil, err
	}
	rows := make([]map[string]string, 0)
	for {
		rec, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, nil, err
		}
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(rec) {
				row[h] = normalizeCSVField(rec[i])
			} else {
				row[h] = ""
			}
		}
		rows = append(rows, row)
	}
	return headers, rows, nil
}

func writeCSV(path string, renamedCols, shuffledCols []string, rows []map[string]string, renameMap map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		return err
	}
	if err := writeCSVRecordPythonStyle(f, renamedCols); err != nil {
		return err
	}
	for _, row := range rows {
		rec := make([]string, 0, len(shuffledCols))
		for _, col := range shuffledCols {
			_ = renameMap[col]
			rec = append(rec, row[col])
		}
		if err := writeCSVRecordPythonStyle(f, rec); err != nil {
			return err
		}
	}
	return nil
}

func slightRename(col string) string {
	out := col
	replacements := [][2]string{
		{"breadcrumbs", "crumbs"},
		{"breadcrumb", "crumb"},
		{"category_path", "category_tree"},
		{"product_is_pharmacy", "is_pharmacy_product"},
		{"rating_count", "reviews_count"},
		{"rating_value", "rating_score"},
		{"price_eur", "price_eur_amt"},
		{"unit_price", "price_per_unit"},
		{"unit_quantity", "pack_qty"},
		{"currency", "currency_code"},
		{"title_subheadline", "title_subline"},
		{"has_", "is_"},
		{"desc_", "details_"},
		{"eyecatchers", "highlights"},
		{"pills", "badges"},
		{"gtin", "gtin_code"},
		{"dan", "dan_code"},
		{"name", "product_name"},
		{"brand", "brand_name"},
	}
	for _, rep := range replacements {
		out = strings.ReplaceAll(out, rep[0], rep[1])
	}
	return out
}

func buildUniqueNames(columns []string) ([]string, map[string]string) {
	renameMap := make(map[string]string, len(columns))
	used := make(map[string]int)
	out := make([]string, 0, len(columns))
	for _, col := range columns {
		candidate := slightRename(col)
		if n, ok := used[candidate]; ok {
			n++
			used[candidate] = n
			candidate = candidate + "_" + strconv.Itoa(n)
		} else {
			used[candidate] = 1
		}
		renameMap[col] = candidate
		out = append(out, candidate)
	}
	return out, renameMap
}

func normalizeCSVField(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

func writeCSVRecordPythonStyle(w io.Writer, rec []string) error {
	for i, field := range rec {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if needsCSVQuote(field) {
			if _, err := io.WriteString(w, `"`); err != nil {
				return err
			}
			escaped := strings.ReplaceAll(field, `"`, `""`)
			if _, err := io.WriteString(w, escaped); err != nil {
				return err
			}
			if _, err := io.WriteString(w, `"`); err != nil {
				return err
			}
		} else {
			if _, err := io.WriteString(w, field); err != nil {
				return err
			}
		}
	}
	_, err := io.WriteString(w, "\r\n")
	return err
}

func needsCSVQuote(s string) bool {
	return strings.ContainsAny(s, ",\"\n\r")
}
