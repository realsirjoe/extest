package main

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const defaultAddr = "127.0.0.1:18744"
const sitemapProtocolMaxURLs = 50000
const defaultSitemapChunkSize = 10000
const searchMinChars = 3
const searchPageSize = 10

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s -path <path-to-sqlite> -id <unique-id-column>\n", os.Args[0])
		flag.PrintDefaults()
	}

	dbPath := flag.String("path", "", "Path to sqlite database")
	idCol := flag.String("id", "", "Name of the unique ID column used for lookup")
	addr := flag.String("addr", defaultAddr, "HTTP listen address")
	sitemapChunkSize := flag.Int("sitemap-chunk-size", defaultSitemapChunkSize, "Max product URLs per sitemap file (capped at 50000)")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("missing -path")
	}
	if *idCol == "" {
		log.Fatal("missing -id column name")
	}
	if *sitemapChunkSize <= 0 {
		*sitemapChunkSize = defaultSitemapChunkSize
	}
	if *sitemapChunkSize > sitemapProtocolMaxURLs {
		*sitemapChunkSize = sitemapProtocolMaxURLs
	}

	if _, err := os.Stat(*dbPath); err != nil {
		log.Fatalf("sqlite path error: %v", err)
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	table, err := firstUserTable(db)
	if err != nil {
		log.Fatalf("find table: %v", err)
	}

	cols, err := tableColumns(db, table)
	if err != nil {
		log.Fatalf("load columns: %v", err)
	}
	if !contains(cols, *idCol) {
		log.Fatalf("id column %q not found in table %q", *idCol, table)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		total, err := countNonEmptyIDs(db, table, *idCol)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Printf("sitemap count error: %v", err)
			return
		}
		baseURL := requestBaseURL(r)
		payload := buildSitemapIndexXML(baseURL, total, *sitemapChunkSize)
		writeXML(w, payload)
	})
	mux.HandleFunc("/sitemaps/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		pageNum, ok := parseProductSitemapPage(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		total, err := countNonEmptyIDs(db, table, *idCol)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Printf("sitemap count error: %v", err)
			return
		}
		if total == 0 {
			http.NotFound(w, r)
			return
		}
		pageCount := (total + *sitemapChunkSize - 1) / *sitemapChunkSize
		if pageNum < 1 || pageNum > pageCount {
			http.NotFound(w, r)
			return
		}
		offset := (pageNum - 1) * *sitemapChunkSize
		ids, err := fetchProductIDsPage(db, table, *idCol, *sitemapChunkSize, offset)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Printf("sitemap page error: %v", err)
			return
		}
		baseURL := requestBaseURL(r)
		payload := buildProductURLSetXML(baseURL, ids)
		writeXML(w, payload)
	})
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" {
			http.NotFound(w, r)
			return
		}
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		page := 1
		var searchData any = nil
		var searchError string
		if q != "" {
			var ok bool
			if len([]rune(q)) < searchMinChars {
				searchError = fmt.Sprintf("query must be at least %d characters", searchMinChars)
			} else if page, ok = parsePageQueryParam(r, "page", 1); !ok {
				searchError = "invalid page"
			} else {
				offset, ok := pageOffset(page, searchPageSize)
				if !ok {
					searchError = "page value is too large"
				} else {
					payload, err := fetchSearchPayload(db, table, cols, *idCol, q, page, searchPageSize, offset)
					if err != nil {
						searchError = "Could not load search results right now."
						log.Printf("search error: %v", err)
					} else {
						searchData = payload
					}
				}
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := searchPageTemplate.Execute(w, map[string]any{
			"title":            "Search | dimi",
			"search_data_json": mustJSONTemplateJS(searchData),
			"search_error":     searchError,
		}); err != nil {
			log.Printf("template error: %v", err)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		payload, err := fetchHomePayload(db, table)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Printf("home payload error: %v", err)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := homePageTemplate.Execute(w, map[string]any{
			"title":          "dimi",
			"home_data_json": mustJSONTemplateJS(payload),
		}); err != nil {
			log.Printf("template error: %v", err)
		}
	})
	mux.HandleFunc("/product/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/product/")
		if id == "" || id == r.URL.Path {
			http.Error(w, "missing product id", http.StatusBadRequest)
			return
		}
		id = strings.TrimSuffix(id, "/")

		row, err := fetchByID(db, table, cols, *idCol, id)
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Printf("fetch error: %v", err)
			return
		}
		similar, err := fetchSimilar(db, table, *idCol, id)
		if errors.Is(err, sql.ErrNoRows) {
			similar = []map[string]any{}
		} else if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			log.Printf("similar error: %v", err)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := productPageTemplate.Execute(w, map[string]any{
			"id":                id,
			"product_data_json": mustJSONTemplateJS(row),
			"similar_data_json": mustJSONTemplateJS(similar),
		}); err != nil {
			log.Printf("template error: %v", err)
		}
	})

	log.Printf("medium-server-1 listening on %s (table=%s id=%s)", *addr, table, *idCol)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func mustJSONTemplateJS(v any) template.JS {
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("json marshal error for template data: %v", err)
		return template.JS("null")
	}
	return template.JS(b)
}

type sitemapIndexXML struct {
	XMLName xml.Name        `xml:"sitemapindex"`
	Xmlns   string          `xml:"xmlns,attr"`
	Items   []sitemapRefXML `xml:"sitemap"`
}

type sitemapRefXML struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod,omitempty"`
}

type urlSetXML struct {
	XMLName xml.Name     `xml:"urlset"`
	Xmlns   string       `xml:"xmlns,attr"`
	Items   []urlItemXML `xml:"url"`
}

type urlItemXML struct {
	Loc string `xml:"loc"`
}

func writeXML(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("xml encode error: %v", err)
	}
}

func buildSitemapIndexXML(baseURL string, total, chunkSize int) sitemapIndexXML {
	if chunkSize <= 0 {
		chunkSize = defaultSitemapChunkSize
	}
	if chunkSize > sitemapProtocolMaxURLs {
		chunkSize = sitemapProtocolMaxURLs
	}
	pageCount := 0
	if total > 0 {
		pageCount = (total + chunkSize - 1) / chunkSize
	}
	items := make([]sitemapRefXML, 0, max(pageCount, 1))
	if pageCount == 0 {
		pageCount = 1
	}
	now := time.Now().UTC().Format("2006-01-02")
	for i := 1; i <= pageCount; i++ {
		items = append(items, sitemapRefXML{
			Loc:     fmt.Sprintf("%s/sitemaps/products-%d.xml", baseURL, i),
			LastMod: now,
		})
	}
	return sitemapIndexXML{
		Xmlns: "http://www.sitemaps.org/schemas/sitemap/0.9",
		Items: items,
	}
}

func buildProductURLSetXML(baseURL string, ids []string) urlSetXML {
	items := make([]urlItemXML, 0, len(ids))
	for _, id := range ids {
		items = append(items, urlItemXML{
			Loc: fmt.Sprintf("%s/product/%s", baseURL, id),
		})
	}
	return urlSetXML{
		Xmlns: "http://www.sitemaps.org/schemas/sitemap/0.9",
		Items: items,
	}
}

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		if i := strings.Index(proto, ","); i >= 0 {
			proto = proto[:i]
		}
		scheme = strings.TrimSpace(proto)
	} else if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = "127.0.0.1:8080"
	}
	return scheme + "://" + host
}

func parseProductSitemapPage(path string) (int, bool) {
	const prefix = "/sitemaps/products-"
	const suffix = ".xml"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	if raw == "" || strings.Contains(raw, "/") {
		return 0, false
	}
	n := 0
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = (n * 10) + int(ch-'0')
	}
	if n <= 0 {
		return 0, false
	}
	return n, true
}

func countNonEmptyIDs(db *sql.DB, table, idCol string) (int, error) {
	q := fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL AND TRIM(CAST(%s AS TEXT)) != ''`,
		quoteIdent(table), quoteIdent(idCol), quoteIdent(idCol),
	)
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func fetchProductIDsPage(db *sql.DB, table, idCol string, limit, offset int) ([]string, error) {
	if limit <= 0 {
		limit = defaultSitemapChunkSize
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s
		 WHERE %s IS NOT NULL AND TRIM(CAST(%s AS TEXT)) != ''
		 ORDER BY %s
		 LIMIT ? OFFSET ?`,
		quoteIdent(idCol),
		quoteIdent(table),
		quoteIdent(idCol),
		quoteIdent(idCol),
		quoteIdent(idCol),
	)
	rows, err := db.Query(q, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		s := strings.TrimSpace(fmt.Sprint(normalizeValue(v)))
		if s == "" || s == "<nil>" {
			continue
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func firstUserTable(db *sql.DB) (string, error) {
	const q = `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1`
	var name string
	if err := db.QueryRow(q).Scan(&name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("no user tables found")
		}
		return "", err
	}
	return name, nil
}

func tableColumns(db *sql.DB, table string) ([]string, error) {
	q := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table))
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for table %q", table)
	}
	return cols, nil
}

func fetchByID(db *sql.DB, table string, cols []string, idCol, id string) (map[string]any, error) {
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? LIMIT 1", joinIdents(cols), quoteIdent(table), quoteIdent(idCol))
	row := db.QueryRow(q, id)

	values := make([]any, len(cols))
	scans := make([]any, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}
	if err := row.Scan(scans...); err != nil {
		return nil, err
	}

	out := make(map[string]any, len(cols))
	for i, col := range cols {
		out[col] = normalizeValue(values[i])
	}
	return out, nil
}

func fetchSimilar(db *sql.DB, table, idCol, id string) ([]map[string]any, error) {
	idColQ := quoteIdent(idCol)
	tableQ := quoteIdent(table)

	var brand, category sql.NullString
	metaQ := fmt.Sprintf("SELECT brand, category_path FROM %s WHERE %s = ? LIMIT 1", tableQ, idColQ)
	if err := db.QueryRow(metaQ, id).Scan(&brand, &category); err != nil {
		return nil, err
	}

	brandVal := strings.TrimSpace(brand.String)
	catVal := strings.TrimSpace(category.String)
	if brandVal == "" && catVal == "" {
		return []map[string]any{}, nil
	}

	baseSelect := fmt.Sprintf("SELECT gtin, name, brand, price_eur, currency, category_path, rating_value, rating_count FROM %s WHERE %s != ?", tableQ, idColQ)
	var args []any
	args = append(args, id)

	var where string
	if brandVal != "" && catVal != "" {
		where = " AND (category_path = ? OR brand = ?)"
		args = append(args, catVal, brandVal)
	} else if catVal != "" {
		where = " AND category_path = ?"
		args = append(args, catVal)
	} else {
		where = " AND brand = ?"
		args = append(args, brandVal)
	}

	order := " ORDER BY CASE WHEN category_path = ? THEN 0 ELSE 1 END, rating_value DESC, rating_count DESC LIMIT 8"
	if catVal == "" {
		order = " ORDER BY rating_value DESC, rating_count DESC LIMIT 8"
	}

	if catVal != "" {
		args = append(args, catVal)
	}

	q := baseSelect + where + order
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var gtin, name, brandOut, currency, categoryOut sql.NullString
		var price sql.NullFloat64
		var ratingVal sql.NullFloat64
		var ratingCount sql.NullInt64
		if err := rows.Scan(&gtin, &name, &brandOut, &price, &currency, &categoryOut, &ratingVal, &ratingCount); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"gtin":          gtin.String,
			"name":          name.String,
			"brand":         brandOut.String,
			"price_eur":     price.Float64,
			"currency":      currency.String,
			"category_path": categoryOut.String,
			"rating_value":  ratingVal.Float64,
			"rating_count":  ratingCount.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type homePayload struct {
	GeneratedAt string        `json:"generated_at"`
	Table       string        `json:"table"`
	Sections    []homeSection `json:"sections"`
}

type homeSection struct {
	ID          string           `json:"id"`
	Title       string           `json:"title"`
	Description string           `json:"description,omitempty"`
	Items       []map[string]any `json:"items"`
}

type searchPayload struct {
	Query          string           `json:"query"`
	MinQueryLength int              `json:"min_query_length"`
	Page           int              `json:"page"`
	MinPage        int              `json:"min_page"`
	MaxPage        int              `json:"max_page"`
	PerPage        int              `json:"per_page"`
	Offset         int              `json:"offset"`
	Total          int              `json:"total"`
	TotalPages     int              `json:"total_pages"`
	Returned       int              `json:"returned"`
	SearchFields   []string         `json:"search_fields"`
	Items          []map[string]any `json:"items"`
}

func fetchHomePayload(db *sql.DB, table string) (homePayload, error) {
	sections := []homeSection{}

	queries := []struct {
		id, title, desc, where, order string
		args                          []any
		limit                         int
	}{
		{
			id:    "top-rated",
			title: "Top Rated Picks",
			desc:  "Strong ratings with enough review volume to be meaningful.",
			where: "price_eur IS NOT NULL AND rating_count >= 20",
			order: "rating_value DESC, rating_count DESC, price_eur ASC",
			limit: 12,
		},
		{
			id:    "most-reviewed",
			title: "Most Reviewed",
			desc:  "Products with the highest number of ratings.",
			where: "price_eur IS NOT NULL AND rating_count >= 1",
			order: "rating_count DESC, rating_value DESC, price_eur ASC",
			limit: 12,
		},
		{
			id:    "budget-finds",
			title: "Budget Finds",
			desc:  "Low-price items with good customer feedback.",
			where: "price_eur IS NOT NULL AND price_eur <= 5 AND rating_count >= 5",
			order: "rating_value DESC, rating_count DESC, price_eur ASC",
			limit: 12,
		},
		{
			id:    "pharmacy-picks",
			title: "Pharmacy Picks",
			desc:  "A selection from pharmacy-tagged products.",
			where: "product_is_pharmacy = 1 AND price_eur IS NOT NULL",
			order: "rating_value DESC, rating_count DESC, price_eur ASC",
			limit: 12,
		},
		{
			id:    "featured-badges",
			title: "Featured & Highlighted",
			desc:  "Products with eyecatchers or pill labels.",
			where: "(has_eyecatchers = 1 OR has_pills = 1) AND price_eur IS NOT NULL",
			order: "rating_count DESC, rating_value DESC, price_eur ASC",
			limit: 12,
		},
	}

	for _, q := range queries {
		items, err := fetchHomeSectionItems(db, table, q.where, q.order, q.limit, q.args...)
		if err != nil {
			return homePayload{}, err
		}
		if len(items) == 0 {
			continue
		}
		sections = append(sections, homeSection{
			ID:          q.id,
			Title:       q.title,
			Description: q.desc,
			Items:       items,
		})
	}

	return homePayload{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Table:       table,
		Sections:    sections,
	}, nil
}

func fetchHomeSectionItems(db *sql.DB, table, where, order string, limit int, args ...any) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 12
	}

	tableQ := quoteIdent(table)
	q := fmt.Sprintf(
		`SELECT gtin, name, brand, price_eur, currency, category_path, rating_value, rating_count
		 FROM %s`, tableQ,
	)
	if strings.TrimSpace(where) != "" {
		q += " WHERE " + where
	}
	if strings.TrimSpace(order) != "" {
		q += " ORDER BY " + order
	}
	q += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var gtin, name, brand, currency, category sql.NullString
		var price sql.NullFloat64
		var ratingVal sql.NullFloat64
		var ratingCount sql.NullInt64
		if err := rows.Scan(&gtin, &name, &brand, &price, &currency, &category, &ratingVal, &ratingCount); err != nil {
			return nil, err
		}

		gtinVal := gtin.String
		out = append(out, map[string]any{
			"gtin":          gtinVal,
			"name":          name.String,
			"brand":         brand.String,
			"price_eur":     price.Float64,
			"currency":      currency.String,
			"category_path": category.String,
			"rating_value":  ratingVal.Float64,
			"rating_count":  ratingCount.Int64,
			"product_path":  "/product/" + gtinVal,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func fetchSearchPayload(db *sql.DB, table string, cols []string, idCol, query string, page, perPage, offset int) (searchPayload, error) {
	searchFields := make([]string, 0, 3)
	for _, c := range []string{"name", "brand", "category_path"} {
		if contains(cols, c) {
			searchFields = append(searchFields, c)
		}
	}
	if len(searchFields) == 0 {
		return searchPayload{}, fmt.Errorf("no searchable columns available")
	}

	idSelectName := "gtin"
	if !contains(cols, "gtin") {
		idSelectName = idCol
	}
	if !contains(cols, idSelectName) {
		return searchPayload{}, fmt.Errorf("id column %q not found for search result selection", idSelectName)
	}

	pattern := "%" + escapeLikePattern(query) + "%"
	whereParts := make([]string, 0, len(searchFields))
	whereArgs := make([]any, 0, len(searchFields))
	for _, f := range searchFields {
		whereParts = append(whereParts, fmt.Sprintf("%s LIKE ? ESCAPE '\\'", quoteIdent(f)))
		whereArgs = append(whereArgs, pattern)
	}
	whereClause := strings.Join(whereParts, " OR ")
	tableQ := quoteIdent(table)

	countQ := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE (%s)", tableQ, whereClause)
	var total int
	if err := db.QueryRow(countQ, whereArgs...).Scan(&total); err != nil {
		return searchPayload{}, err
	}

	items, err := fetchSearchItems(db, table, searchFields, idSelectName, perPage, offset, whereClause, whereArgs...)
	if err != nil {
		return searchPayload{}, err
	}
	totalPages := 0
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}

	return searchPayload{
		Query:          query,
		MinQueryLength: searchMinChars,
		Page:           page,
		MinPage:        1,
		MaxPage:        totalPages,
		PerPage:        perPage,
		Offset:         offset,
		Total:          total,
		TotalPages:     totalPages,
		Returned:       len(items),
		SearchFields:   searchFields,
		Items:          items,
	}, nil
}

func fetchSearchItems(db *sql.DB, table string, searchFields []string, idCol string, limit, offset int, whereClause string, whereArgs ...any) ([]map[string]any, error) {
	tableQ := quoteIdent(table)
	idColQ := quoteIdent(idCol)
	orderClauses := make([]string, 0, len(searchFields)+3)
	for _, f := range searchFields {
		fq := quoteIdent(f)
		orderClauses = append(orderClauses, fmt.Sprintf("CASE WHEN %s LIKE ? ESCAPE '\\' THEN 0 ELSE 1 END", fq))
	}
	orderClauses = append(orderClauses, "rating_count DESC", "rating_value DESC", quoteIdent("name")+" ASC")
	orderClause := strings.Join(orderClauses, ", ")

	args := make([]any, 0, len(whereArgs)+len(searchFields)+2)
	args = append(args, whereArgs...)
	// Use q% ranking pattern derived from the substring pattern input.
	if len(whereArgs) > 0 {
		if substrPattern, ok := whereArgs[0].(string); ok {
			prefix := prefixLikePatternFromSubstringPattern(substrPattern)
			for range searchFields {
				args = append(args, prefix)
			}
		}
	}
	args = append(args, limit, offset)

	q := fmt.Sprintf(
		`SELECT %s, name, brand, price_eur, currency, category_path, rating_value, rating_count
		 FROM %s
		 WHERE (%s)
		 ORDER BY %s
		 LIMIT ? OFFSET ?`,
		idColQ, tableQ, whereClause, orderClause,
	)

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var idVal, name, brand, currency, category sql.NullString
		var price sql.NullFloat64
		var ratingVal sql.NullFloat64
		var ratingCount sql.NullInt64
		if err := rows.Scan(&idVal, &name, &brand, &price, &currency, &category, &ratingVal, &ratingCount); err != nil {
			return nil, err
		}
		id := idVal.String
		item := map[string]any{
			"id":            id,
			"name":          name.String,
			"brand":         brand.String,
			"price_eur":     price.Float64,
			"currency":      currency.String,
			"category_path": category.String,
			"rating_value":  ratingVal.Float64,
			"rating_count":  ratingCount.Int64,
			"product_path":  "/product/" + id,
		}
		if idCol == "gtin" {
			item["gtin"] = id
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func escapeLikePattern(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(s)
}

func prefixLikePatternFromSubstringPattern(substrPattern string) string {
	trimmed := strings.TrimPrefix(strings.TrimSuffix(substrPattern, "%"), "%")
	return trimmed + "%"
}

func parsePageQueryParam(r *http.Request, key string, fallback int) (int, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback, true
	}
	n64, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n64 < 1 {
		return 0, false
	}
	if n64 > maxIntValue() {
		return 0, false
	}
	return int(n64), true
}

func pageOffset(page, perPage int) (int, bool) {
	if page < 1 || perPage < 1 {
		return 0, false
	}
	p := int64(page - 1)
	sz := int64(perPage)
	if p > maxIntValue()/sz {
		return 0, false
	}
	return int(p * sz), true
}

func maxIntValue() int64 {
	return int64(^uint(0) >> 1)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("encode error: %v", err)
	}
}

func normalizeValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(time.RFC3339Nano)
	default:
		return v
	}
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func joinIdents(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c)
	}
	return strings.Join(parts, ", ")
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

var productPageTemplate = template.Must(template.New("product").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Product {{ .id }} | dimi</title>
  <style>
    :root {
      --bg: #f5f3ef;
      --card: #ffffff;
      --ink: #0f172a;
      --muted: #64748b;
      --accent: #0f766e;
      --accent-2: #f97316;
      --border: #e2e8f0;
      --shadow: 0 12px 30px rgba(15, 23, 42, 0.10);
    }
    body {
      margin: 0;
      background: radial-gradient(circle at 15% 20%, #fef3c7, transparent 40%),
                  radial-gradient(circle at 85% 10%, #d1fae5, transparent 45%),
                  var(--bg);
      color: var(--ink);
      font-family: "Georgia", "Times New Roman", serif;
    }
    .page-shell { max-width: 1180px; margin: 0 auto; padding: 20px 20px 0; }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      padding: 10px 14px;
      border: 1px solid rgba(15, 23, 42, 0.12);
      background: rgba(255,255,255,0.72);
      border-radius: 999px;
      backdrop-filter: blur(6px);
      position: sticky;
      top: 10px;
      z-index: 10;
    }
    .logo {
      font-size: 14px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      font-weight: 700;
      color: var(--accent);
      text-decoration: none;
    }
    .search-form {
      display: flex;
      align-items: center;
      gap: 8px;
      flex: 1 1 460px;
      min-width: 240px;
      max-width: 700px;
      margin: 0 8px;
    }
    .search-input {
      flex: 1;
      min-width: 0;
      border: 1px solid rgba(15, 23, 42, 0.12);
      background: rgba(255,255,255,0.95);
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 14px;
      outline: none;
      color: #0f172a;
    }
    .search-input:focus {
      border-color: rgba(15, 118, 110, 0.4);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .search-submit {
      border: 1px solid rgba(15, 118, 110, 0.20);
      background: #0f766e;
      color: #fff;
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 13px;
      cursor: pointer;
      white-space: nowrap;
    }
    .top-actions { display: flex; gap: 8px; }
    .chip {
      display: inline-flex;
      align-items: center;
      padding: 8px 12px;
      border: 1px solid rgba(15, 23, 42, 0.12);
      border-radius: 999px;
      background: rgba(255,255,255,0.85);
      font-size: 13px;
      text-decoration: none;
      color: #1f2937;
    }
    .wrap { max-width: 1040px; margin: 40px auto 64px; padding: 0 20px; }
    .crumbs { font-size: 14px; color: var(--muted); margin-bottom: 14px; text-transform: capitalize; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 26px;
      box-shadow: var(--shadow);
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
      gap: 30px;
    }
    .media {
      border-radius: 14px;
      background: #f8fafc;
      border: 1px solid var(--border);
      aspect-ratio: 4 / 3;
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--muted);
      font-size: 14px;
      overflow: hidden;
      max-width: 520px;
      margin: 0 auto;
    }
    .media img { width: 100%; height: 100%; object-fit: contain; border-radius: 14px; padding: 18px; background: #ffffff; }
    .brand { font-size: 12px; letter-spacing: 0.18em; text-transform: uppercase; color: var(--accent); margin-bottom: 8px; }
    h1 { font-size: clamp(24px, 3.2vw, 34px); margin: 0 0 12px; line-height: 1.2; word-break: break-word; }
    .price-row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; margin: 14px 0; }
    .price { font-size: 26px; font-weight: 700; }
    .pill { font-size: 12px; color: var(--accent-2); border: 1px solid #fed7aa; background: #fff7ed; padding: 4px 10px; border-radius: 999px; }
    .meta { color: var(--muted); font-size: 14px; margin-bottom: 16px; }
    .meta span { display: inline-block; margin-right: 12px; }
    .cta {
      display: inline-block;
      background: var(--accent);
      color: white;
      padding: 10px 18px;
      border-radius: 999px;
      text-decoration: none;
      font-size: 14px;
      letter-spacing: 0.02em;
    }
    .cta-secondary {
      display: inline-block;
      background: #ffffff;
      color: var(--accent);
      padding: 10px 18px;
      border-radius: 999px;
      text-decoration: none;
      font-size: 14px;
      border: 1px solid #99f6e4;
      margin-left: 8px;
    }
    .desc { margin-top: 18px; line-height: 1.7; font-size: 16px; color: #1f2937; max-width: 60ch; }
    .rating-box {
      margin-top: 16px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: #f8fafc;
      padding: 12px 14px;
    }
    .rating-label { font-size: 11px; letter-spacing: 0.14em; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; }
    .rating-row { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .rating-stars { color: #f59e0b; letter-spacing: 1px; font-size: 16px; }
    .rating-text { color: #334155; font-size: 14px; }
    .specs { margin-top: 18px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 18px; font-size: 14px; color: var(--muted); }
    .specs div { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .details {
      margin-top: 18px;
    }
    .details h2 { margin: 0 0 6px; font-size: 18px; }
    .details-sub { color: var(--muted); font-size: 13px; margin-bottom: 12px; }
    .details-table-wrap { overflow: auto; border: 1px solid rgba(15,23,42,0.08); border-radius: 12px; background: #fff; }
    .details-table { width: 100%; border-collapse: collapse; font-size: 14px; }
    .details-table th, .details-table td { padding: 10px 12px; text-align: left; vertical-align: top; border-bottom: 1px solid rgba(15,23,42,0.06); }
    .details-table th { width: 28%; min-width: 180px; color: var(--muted); font-weight: 600; background: #fcfcfd; }
    .details-table td { color: #111827; white-space: pre-wrap; word-break: break-word; }
    .details-table tr:last-child th, .details-table tr:last-child td { border-bottom: 0; }
    .recs {
      margin-top: 26px;
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 20px;
      backdrop-filter: blur(4px);
    }
    .recs h2 { margin: 0 0 6px; font-size: 20px; }
    .recs-sub { color: var(--muted); font-size: 14px; margin-bottom: 14px; }
    .recs-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .rec-card {
      display: block;
      text-decoration: none;
      color: inherit;
      background: #fff;
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      min-height: 118px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.05);
      transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
    }
    .rec-card:hover {
      transform: translateY(-2px);
      box-shadow: 0 12px 20px rgba(15, 23, 42, 0.08);
      border-color: #cbd5e1;
    }
    .rec-brand {
      font-size: 11px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: var(--accent);
      margin-bottom: 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .rec-name {
      font-size: 14px;
      line-height: 1.35;
      color: #111827;
      margin-bottom: 8px;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
      min-height: 56px;
    }
    .rec-meta {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .rec-price { color: var(--ink); font-weight: 700; font-size: 13px; }
    .recs-status { color: var(--muted); font-size: 14px; }
    @media (max-width: 900px) {
      .card { grid-template-columns: 1fr; }
      .desc { max-width: none; }
      .specs { grid-template-columns: 1fr; }
      .recs-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 560px) {
      .recs-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      .topbar { border-radius: 18px; }
    }
  </style>
</head>
<body>
  <div class="page-shell">
    <div class="topbar">
      <a class="logo" href="/">dimi</a>
      <form class="search-form" action="/search" method="get" role="search">
        <input class="search-input" type="search" name="q" minlength="3" required placeholder="Search products, brands, categories" />
        <button class="search-submit" type="submit">Search</button>
      </form>
      <div class="top-actions">
        <a class="chip" href="/">Offers</a>
        <a class="chip" href="#">Account</a>
      </div>
    </div>
  </div>
  <div class="wrap">
    <div class="crumbs" id="product-crumbs">Loading product...</div>
    <div class="card">
      <div class="media" id="product-media">
        <span id="product-media-fallback">Loading image...</span>
      </div>
      <div>
        <div class="brand" id="product-brand">Loading brand…</div>
        <h1 id="product-name">Loading product…</h1>
        <div class="price-row">
          <div class="price" id="product-price">Loading price…</div>
          <div class="pill">In stock</div>
        </div>
        <div class="meta" id="product-meta">
          <span>Product ID: <span id="product-id">{{ .id }}</span></span>
          <span id="product-category-wrap" hidden>Category: <span id="product-category"></span></span>
        </div>
        <a class="cta" href="#">Add to cart</a>
        <a class="cta-secondary" href="#">Wishlist</a>
        <div class="desc" id="product-desc" hidden></div>
        <div class="rating-box" id="product-rating" hidden>
          <div class="rating-label">Customer Rating</div>
          <div class="rating-row">
            <div class="rating-stars" id="product-rating-stars"></div>
            <div class="rating-text" id="product-rating-text"></div>
          </div>
        </div>
        <div class="meta" id="product-load-status">Loading product details from API…</div>
        <div class="specs">
          <div>Shipping: 2-4 days</div>
          <div>Returns: 30 days</div>
          <div>Support: Email & chat</div>
          <div>Secure checkout</div>
        </div>
        <section class="details" id="product-details" hidden>
          <h2>Additional details</h2>
          <div class="details-sub">Non-standard product fields provided by this catalog entry.</div>
          <div class="details-table-wrap">
            <table class="details-table">
              <tbody id="product-details-body"></tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
    <section class="recs" id="similar-products">
      <h2>Products you may also like</h2>
      <div class="recs-sub">Related suggestions loaded from the product API.</div>
      <div class="recs-status" id="similar-status">Loading suggestions...</div>
      <div class="recs-grid" id="similar-grid" hidden></div>
    </section>
  </div>
  <script>
    (function () {
      var productId = {{ .id }};
      var productApiUrl = "/api/product/" + encodeURIComponent(productId);
      var statusEl = document.getElementById("similar-status");
      var gridEl = document.getElementById("similar-grid");
      var sectionEl = document.getElementById("similar-products");
      var crumbsEl = document.getElementById("product-crumbs");
      var mediaEl = document.getElementById("product-media");
      var mediaFallbackEl = document.getElementById("product-media-fallback");
      var brandEl = document.getElementById("product-brand");
      var nameEl = document.getElementById("product-name");
      var priceEl = document.getElementById("product-price");
      var catWrapEl = document.getElementById("product-category-wrap");
      var catEl = document.getElementById("product-category");
      var descEl = document.getElementById("product-desc");
      var ratingBoxEl = document.getElementById("product-rating");
      var ratingStarsEl = document.getElementById("product-rating-stars");
      var ratingTextEl = document.getElementById("product-rating-text");
      var detailsSectionEl = document.getElementById("product-details");
      var detailsBodyEl = document.getElementById("product-details-body");
      var loadStatusEl = document.getElementById("product-load-status");
      if (!productId || !statusEl || !gridEl || !sectionEl) return;

      function escapeHtml(s) {
        return String(s ?? "").replace(/[&<>\"']/g, function (ch) {
          return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" })[ch];
        });
      }

      function formatPrice(item) {
        if (typeof item.price_eur !== "number" || Number.isNaN(item.price_eur)) return "";
        try {
          return new Intl.NumberFormat("de-DE", {
            style: "currency",
            currency: item.currency || "EUR",
            minimumFractionDigits: 2
          }).format(item.price_eur);
        } catch (_) {
          return item.price_eur.toFixed(2) + " " + (item.currency || "EUR");
        }
      }

      function firstNonEmpty() {
        for (var i = 0; i < arguments.length; i++) {
          var v = arguments[i];
          if (v === null || v === undefined) continue;
          var s = String(v).trim();
          if (s) return s;
        }
        return "";
      }

      function setText(el, value, fallback) {
        if (!el) return;
        el.textContent = firstNonEmpty(value, fallback);
      }

      function formatMainPrice(row) {
        var raw = firstNonEmpty(row.price_raw);
        if (raw) return raw;
        if (typeof row.price_eur === "number" && !Number.isNaN(row.price_eur)) {
          return formatPrice({ price_eur: row.price_eur, currency: row.currency || "EUR" }) || "Price not available";
        }
        var meta = firstNonEmpty(row.metadata_price_eur);
        return meta || "Price not available";
      }

      function setMedia(row, name) {
        if (!mediaEl) return;
        var src = firstNonEmpty(row.image, row.image_url, row.img, row.thumbnail);
        if (!src) {
          if (mediaFallbackEl) mediaFallbackEl.textContent = "No image";
          return;
        }
        mediaEl.innerHTML = '<img src="' + escapeHtml(src) + '" alt="' + escapeHtml(name || "Product") + '" />';
      }

      function parseNumber(v) {
        if (typeof v === "number" && Number.isFinite(v)) return v;
        if (typeof v === "string") {
          var n = Number(v.trim());
          if (Number.isFinite(n)) return n;
        }
        return null;
      }

      function renderRating(row) {
        if (!ratingBoxEl || !ratingStarsEl || !ratingTextEl) return;
        var rv = parseNumber(row.rating_value);
        var rc = parseNumber(row.rating_count);
        if (!(rv > 0) && !(rc > 0)) {
          ratingBoxEl.hidden = true;
          return;
        }
        var stars = "";
        if (rv > 0) {
          var rounded = Math.max(0, Math.min(5, rv));
          var full = Math.round(rounded);
          for (var i = 0; i < 5; i++) stars += i < full ? "★" : "☆";
          ratingStarsEl.textContent = stars + " " + rounded.toFixed(1);
          ratingTextEl.textContent = (rc > 0 ? (Math.round(rc) + " ratings") : "Customer reviews available");
        } else {
          ratingStarsEl.textContent = "";
          ratingTextEl.textContent = Math.round(rc) + " ratings";
        }
        ratingBoxEl.hidden = false;
      }

      function formatFieldLabel(key) {
        return String(key || "")
          .replace(/^desc_/, "description_")
          .replace(/_/g, " ")
          .replace(/\b\w/g, function (ch) { return ch.toUpperCase(); });
      }

      function isMeaningfulValue(v) {
        if (v === null || v === undefined) return false;
        if (typeof v === "string") return v.trim() !== "";
        return true;
      }

      function valueText(v) {
        if (v === null || v === undefined) return "";
        if (typeof v === "object") {
          try { return JSON.stringify(v); } catch (_) { return String(v); }
        }
        return String(v);
      }

      function renderAdditionalDetails(row) {
        if (!detailsSectionEl || !detailsBodyEl) return;
        var excluded = {
          gtin: true, dan: true,
          name: true, title_headline: true,
          brand: true, seo_brand: true,
          price_raw: true, price_eur: true, metadata_price_eur: true, currency: true,
          category_path: true, seo_category: true,
          image: true, image_url: true, img: true, thumbnail: true,
          desc_productbeschreibung: true, metadata_description: true,
          rating_value: true, rating_count: true
        };
        var rows = [];
        Object.keys(row || {}).sort().forEach(function (key) {
          if (excluded[key]) return;
          var val = row[key];
          if (!isMeaningfulValue(val)) return;
          rows.push(
            "<tr><th>" + escapeHtml(formatFieldLabel(key)) + "</th><td>" + escapeHtml(valueText(val)) + "</td></tr>"
          );
        });
        if (rows.length === 0) {
          detailsBodyEl.innerHTML = "";
          detailsSectionEl.hidden = true;
          return;
        }
        detailsBodyEl.innerHTML = rows.join("");
        detailsSectionEl.hidden = false;
      }

      function hydrateProduct(row) {
        var name = firstNonEmpty(row.name, row.title_headline, "Product " + productId);
        var brand = firstNonEmpty(row.brand, row.seo_brand, "Unknown brand");
        var category = firstNonEmpty(row.category_path, row.seo_category);
        var desc = firstNonEmpty(row.desc_productbeschreibung, row.metadata_description);

        document.title = name + " | dimi";
        if (crumbsEl) crumbsEl.textContent = category || "Product details";
        setMedia(row, name);
        setText(brandEl, brand, "Unknown brand");
        setText(nameEl, name, "Product");
        setText(priceEl, formatMainPrice(row), "Price not available");

        if (catWrapEl && catEl) {
          if (category) {
            catEl.textContent = category;
            catWrapEl.hidden = false;
          } else {
            catWrapEl.hidden = true;
          }
        }

        if (descEl) {
          if (desc) {
            descEl.textContent = desc;
            descEl.hidden = false;
          } else {
            descEl.hidden = true;
          }
        }
        renderRating(row);
        renderAdditionalDetails(row);
        if (loadStatusEl) {
          loadStatusEl.hidden = true;
        }
      }

      try {
        var productData = {{ .product_data_json }};
        hydrateProduct(productData || {});
      } catch (err) {
        if (loadStatusEl) {
          loadStatusEl.hidden = false;
          loadStatusEl.textContent = "Could not render product details right now.";
        }
        if (crumbsEl) crumbsEl.textContent = "Product details";
        if (brandEl) brandEl.textContent = "Unavailable";
        if (nameEl) nameEl.textContent = "Product " + productId;
        if (priceEl) priceEl.textContent = "Price not available";
        if (mediaFallbackEl) mediaFallbackEl.textContent = "No image";
      }

      try {
        var items = {{ .similar_data_json }};
        if (!Array.isArray(items) || items.length === 0) {
          sectionEl.hidden = true;
          return;
        }
        gridEl.innerHTML = items.map(function (item) {
          var gtin = item.gtin || "";
          var name = escapeHtml(item.name || "Product");
          var brand = escapeHtml(item.brand || "Unknown brand");
          var price = escapeHtml(formatPrice(item));
          var rating = (typeof item.rating_value === "number" && item.rating_value > 0)
            ? ("★ " + item.rating_value.toFixed(1))
            : "";
          return (
            '<a class="rec-card" href="/product/' + encodeURIComponent(gtin) + '">' +
              '<div class="rec-brand">' + brand + '</div>' +
              '<div class="rec-name">' + name + '</div>' +
              '<div class="rec-meta">' +
                '<span class="rec-price">' + price + '</span>' +
                '<span>' + escapeHtml(rating) + '</span>' +
              '</div>' +
            '</a>'
          );
        }).join("");
        statusEl.hidden = true;
        gridEl.hidden = false;
      } catch (_) {
        statusEl.textContent = "Could not render suggestions right now.";
      }
    })();
  </script>
</body>
</html>`))

var homePageTemplate = template.Must(template.New("home").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ .title }}</title>
  <style>
    :root {
      --bg: #f3f0e7;
      --ink: #0f172a;
      --muted: #667085;
      --line: rgba(15, 23, 42, 0.12);
      --card: rgba(255,255,255,0.9);
      --brand: #0f766e;
      --brand-2: #ea580c;
      --shadow: 0 18px 40px rgba(15, 23, 42, 0.10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Georgia", "Times New Roman", serif;
      background:
        radial-gradient(1000px 500px at 8% -5%, rgba(245, 158, 11, 0.18), transparent 60%),
        radial-gradient(900px 500px at 95% 0%, rgba(16, 185, 129, 0.16), transparent 60%),
        linear-gradient(180deg, #f7f4ec 0%, #f3f0e7 40%, #efede6 100%);
    }
    a { color: inherit; }
    .shell { max-width: 1180px; margin: 0 auto; padding: 20px 20px 56px; }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      padding: 10px 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.7);
      border-radius: 999px;
      backdrop-filter: blur(6px);
      position: sticky;
      top: 10px;
      z-index: 10;
    }
    .logo {
      font-size: 14px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      font-weight: 700;
      color: var(--brand);
    }
    .top-actions { display: flex; gap: 8px; }
    .search-form {
      display: flex;
      align-items: center;
      gap: 8px;
      flex: 1 1 360px;
      min-width: 240px;
      max-width: 560px;
      margin: 0 8px;
    }
    .search-input {
      flex: 1;
      min-width: 0;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.92);
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 14px;
      color: #0f172a;
      outline: none;
    }
    .search-input:focus {
      border-color: rgba(15, 118, 110, 0.4);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .search-submit {
      border: 1px solid rgba(15, 118, 110, 0.20);
      background: #0f766e;
      color: #fff;
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 13px;
      cursor: pointer;
      white-space: nowrap;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      padding: 8px 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.85);
      font-size: 13px;
      text-decoration: none;
      color: #1f2937;
    }
    .hero {
      margin-top: 18px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background:
        radial-gradient(circle at 15% 25%, rgba(254, 243, 199, 0.9), transparent 45%),
        radial-gradient(circle at 90% 20%, rgba(209, 250, 229, 0.8), transparent 50%),
        rgba(255,255,255,0.78);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .hero-inner {
      display: grid;
      grid-template-columns: 1.25fr 0.9fr;
      gap: 18px;
      padding: 28px;
    }
    .eyebrow {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      color: var(--brand);
      margin-bottom: 10px;
    }
    h1 {
      margin: 0 0 12px;
      font-size: clamp(30px, 4vw, 48px);
      line-height: 1.03;
      max-width: 16ch;
    }
    .hero-copy {
      font-size: 16px;
      line-height: 1.6;
      color: #334155;
      max-width: 54ch;
      margin-bottom: 18px;
    }
    .hero-cta { display: flex; gap: 10px; flex-wrap: wrap; }
    .btn {
      border-radius: 999px;
      padding: 10px 16px;
      text-decoration: none;
      font-size: 14px;
      border: 1px solid transparent;
      cursor: pointer;
    }
    .btn-primary {
      background: var(--brand);
      color: #fff;
      box-shadow: 0 10px 20px rgba(15, 118, 110, 0.25);
    }
    .btn-secondary {
      background: rgba(255,255,255,0.85);
      color: var(--brand);
      border-color: rgba(15, 118, 110, 0.2);
    }
    .hero-panel {
      border: 1px solid rgba(15, 23, 42, 0.08);
      border-radius: 18px;
      background: rgba(255,255,255,0.86);
      padding: 14px;
      align-self: stretch;
    }
    .hero-panel h2 { margin: 0 0 10px; font-size: 18px; }
    .mini-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .mini-card {
      background: #fff;
      border: 1px solid rgba(15, 23, 42, 0.08);
      border-radius: 12px;
      padding: 10px;
      min-height: 92px;
    }
    .mini-card b { display: block; font-size: 13px; line-height: 1.3; margin-bottom: 6px; }
    .mini-card span { color: var(--muted); font-size: 12px; }
    .status {
      margin-top: 18px;
      border: 1px dashed rgba(15, 23, 42, 0.16);
      border-radius: 14px;
      padding: 14px;
      background: rgba(255,255,255,0.55);
      color: #475569;
      font-size: 14px;
    }
    .sections {
      margin-top: 26px;
      display: grid;
      gap: 18px;
      scroll-margin-top: 84px;
    }
    .section {
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
      border-radius: 20px;
      box-shadow: 0 10px 25px rgba(15, 23, 42, 0.05);
      overflow: hidden;
    }
    .section-head {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 12px;
      padding: 18px 18px 10px;
    }
    .section-title { margin: 0; font-size: 22px; }
    .section-desc { margin: 4px 0 0; color: var(--muted); font-size: 14px; }
    .section-meta { color: var(--muted); font-size: 12px; white-space: nowrap; }
    .cards {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      padding: 0 18px 18px;
    }
    .card {
      display: block;
      text-decoration: none;
      border: 1px solid rgba(15, 23, 42, 0.10);
      border-radius: 16px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.95), rgba(248,250,252,0.92));
      padding: 12px;
      min-height: 154px;
      transition: transform 140ms ease, box-shadow 140ms ease, border-color 140ms ease;
    }
    .card:hover {
      transform: translateY(-2px);
      border-color: rgba(15, 23, 42, 0.18);
      box-shadow: 0 14px 24px rgba(15, 23, 42, 0.08);
    }
    .card-brand {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--brand);
      margin-bottom: 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .card-name {
      font-size: 14px;
      line-height: 1.35;
      color: #0f172a;
      margin-bottom: 10px;
      min-height: 56px;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .card-category {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .card-foot {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .price {
      color: #0f172a;
      font-weight: 700;
      font-size: 13px;
    }
    .footer-note {
      text-align: center;
      color: var(--muted);
      font-size: 13px;
      margin-top: 22px;
    }
    @media (max-width: 1024px) {
      .hero-inner { grid-template-columns: 1fr; }
      .cards { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 760px) {
      .topbar { border-radius: 18px; }
      .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .mini-grid { grid-template-columns: 1fr; }
      .section-head { align-items: flex-start; flex-direction: column; }
    }
    @media (max-width: 520px) {
      .cards { grid-template-columns: 1fr; }
      .hero-inner { padding: 18px; }
      .section-head { padding: 16px 16px 8px; }
      .cards { padding: 0 16px 16px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="logo">dimi</div>
      <form class="search-form" action="/search" method="get" role="search">
        <input class="search-input" type="search" name="q" minlength="3" required placeholder="Search products, brands, categories" />
        <button class="search-submit" type="submit">Search</button>
      </form>
      <div class="top-actions">
        <a class="chip" href="/">Offers</a>
        <a class="chip" href="#">Account</a>
      </div>
    </div>

    <section class="hero">
      <div class="hero-inner">
        <div>
          <div class="eyebrow">Welcome to dimi</div>
          <h1>Everyday favorites, trending picks, and smart finds</h1>
          <div class="hero-copy">
            Shop curated collections across beauty, baby, home, wellness, and more.
            Explore top-rated products, best value picks, and customer-loved essentials in one place.
          </div>
          <div class="hero-cta">
            <button class="btn btn-primary" id="scroll-sections" type="button">Browse Collections</button>
            <a class="btn btn-secondary" href="#">Shop New Arrivals</a>
          </div>
          <div class="status" id="home-status" hidden></div>
        </div>
        <aside class="hero-panel">
          <h2>Shop by what matters today</h2>
          <div class="mini-grid">
            <div class="mini-card">
              <b>Top Rated</b>
              <span>Popular products with strong customer feedback.</span>
            </div>
            <div class="mini-card">
              <b>Budget Finds</b>
              <span>Everyday essentials at friendly prices.</span>
            </div>
            <div class="mini-card">
              <b>Most Reviewed</b>
              <span>Best-known items shoppers come back to.</span>
            </div>
            <div class="mini-card">
              <b>Pharmacy Picks</b>
              <span>Trusted wellness and care selections.</span>
            </div>
          </div>
        </aside>
      </div>
    </section>

    <main class="sections" id="sections" aria-live="polite"></main>
    <div class="footer-note">Curated for everyday shopping across categories customers love.</div>
  </div>

  <script>
    (function () {
      var statusEl = document.getElementById("home-status");
      var topbarEl = document.querySelector(".topbar");
      var sectionsEl = document.getElementById("sections");
      var scrollBtn = document.getElementById("scroll-sections");

      if (scrollBtn && sectionsEl) {
        scrollBtn.addEventListener("click", function () {
          var targetEl = sectionsEl.querySelector(".section") || sectionsEl;
          function desiredTopOffset() {
            var topbarHeight = topbarEl ? topbarEl.getBoundingClientRect().height : 0;
            var stickyTop = 0;
            if (topbarEl && window.getComputedStyle) {
              var topValue = window.getComputedStyle(topbarEl).top || "0";
              var parsedTop = parseFloat(topValue);
              if (Number.isFinite(parsedTop)) stickyTop = parsedTop;
            }
            return topbarHeight + stickyTop + 18;
          }

          var targetY = window.scrollY + targetEl.getBoundingClientRect().top - desiredTopOffset();
          window.scrollTo({ top: Math.max(0, targetY), behavior: "smooth" });

          // Post-scroll correction: measure actual overlap after sticky positioning settles.
          window.setTimeout(function () {
            var desiredTop = desiredTopOffset();
            var actualTop = targetEl.getBoundingClientRect().top;
            var delta = actualTop - desiredTop;
            if (Math.abs(delta) > 2) {
              window.scrollBy({ top: delta, behavior: "auto" });
            }
          }, 420);
        });
      }

      function escapeHtml(s) {
        return String(s == null ? "" : s).replace(/[&<>\"']/g, function (ch) {
          return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" })[ch];
        });
      }

      function formatPrice(item) {
        if (typeof item.price_eur !== "number" || Number.isNaN(item.price_eur)) return "Price unavailable";
        try {
          return new Intl.NumberFormat("de-DE", {
            style: "currency",
            currency: item.currency || "EUR",
            minimumFractionDigits: 2
          }).format(item.price_eur);
        } catch (_) {
          return item.price_eur.toFixed(2) + " " + (item.currency || "EUR");
        }
      }

      function renderCard(item) {
        var gtin = item.gtin || "";
        var href = item.product_path || ("/product/" + encodeURIComponent(gtin));
        var brand = escapeHtml(item.brand || "Unknown brand");
        var name = escapeHtml(item.name || "Product");
        var category = escapeHtml(item.category_path || "");
        var price = escapeHtml(formatPrice(item));
        var rating = "";
        if (typeof item.rating_value === "number" && item.rating_value > 0) {
          rating = "★ " + item.rating_value.toFixed(1);
          if (typeof item.rating_count === "number" && item.rating_count > 0) {
            rating += " (" + item.rating_count + ")";
          }
        } else if (typeof item.rating_count === "number" && item.rating_count > 0) {
          rating = item.rating_count + " reviews";
        } else {
          rating = "New";
        }

        return '' +
          '<a class="card" href="' + escapeHtml(href) + '">' +
            '<div class="card-brand">' + brand + '</div>' +
            '<div class="card-name">' + name + '</div>' +
            '<div class="card-category">' + category + '</div>' +
            '<div class="card-foot">' +
              '<span class="price">' + price + '</span>' +
              '<span>' + escapeHtml(rating) + '</span>' +
            '</div>' +
          '</a>';
      }

      function renderSection(section) {
        var title = escapeHtml(section.title || "Collection");
        var desc = escapeHtml(section.description || "");
        var id = escapeHtml(section.id || "");
        var items = Array.isArray(section.items) ? section.items : [];
        return '' +
          '<section class="section" data-section-id="' + id + '">' +
            '<div class="section-head">' +
              '<div>' +
                '<h2 class="section-title">' + title + '</h2>' +
                (desc ? '<p class="section-desc">' + desc + '</p>' : '') +
              '</div>' +
              '<div class="section-meta">' + items.length + ' items</div>' +
            '</div>' +
            '<div class="cards">' + items.map(renderCard).join("") + '</div>' +
          '</section>';
      }

      try {
        var data = {{ .home_data_json }};
        var sections = Array.isArray(data.sections) ? data.sections : [];
        if (sections.length === 0) {
          statusEl.hidden = false;
          statusEl.textContent = "No homepage collections available right now.";
          return;
        }
        sectionsEl.innerHTML = sections.map(renderSection).join("");
        statusEl.hidden = true;
      } catch (_) {
        statusEl.hidden = false;
        statusEl.textContent = "Could not load homepage collections right now.";
      }
    })();
  </script>
</body>
</html>`))

var searchPageTemplate = template.Must(template.New("search").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ .title }}</title>
  <style>
    :root {
      --bg: #f3f0e7;
      --ink: #0f172a;
      --muted: #667085;
      --line: rgba(15, 23, 42, 0.12);
      --card: rgba(255,255,255,0.88);
      --brand: #0f766e;
      --shadow: 0 14px 32px rgba(15, 23, 42, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Georgia", "Times New Roman", serif;
      background:
        radial-gradient(900px 500px at 8% -5%, rgba(245, 158, 11, 0.14), transparent 60%),
        radial-gradient(900px 500px at 95% 0%, rgba(16, 185, 129, 0.12), transparent 60%),
        linear-gradient(180deg, #f7f4ec 0%, #f3f0e7 45%, #efede6 100%);
    }
    .shell { max-width: 1180px; margin: 0 auto; padding: 20px 20px 56px; }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      padding: 10px 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.72);
      border-radius: 999px;
      backdrop-filter: blur(6px);
      position: sticky;
      top: 10px;
      z-index: 10;
    }
    .logo {
      font-size: 14px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      font-weight: 700;
      color: var(--brand);
      text-decoration: none;
    }
    .search-form {
      display: flex;
      align-items: center;
      gap: 8px;
      flex: 1 1 460px;
      min-width: 240px;
      max-width: 700px;
      margin: 0 8px;
    }
    .search-input {
      flex: 1;
      min-width: 0;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.95);
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 14px;
      outline: none;
    }
    .search-input:focus {
      border-color: rgba(15, 118, 110, 0.4);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .search-submit {
      border: 1px solid rgba(15, 118, 110, 0.20);
      background: #0f766e;
      color: #fff;
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 13px;
      cursor: pointer;
      white-space: nowrap;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      padding: 8px 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.85);
      font-size: 13px;
      text-decoration: none;
      color: #1f2937;
    }
    .top-actions { display: flex; gap: 8px; }
    .panel {
      margin-top: 18px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: var(--card);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .panel-head {
      padding: 18px 18px 10px;
      border-bottom: 1px solid rgba(15,23,42,0.06);
    }
    .panel-head h1 { margin: 0; font-size: 22px; }
    .panel-sub { margin-top: 6px; color: var(--muted); font-size: 14px; }
    .status {
      margin: 12px 18px 0;
      border: 1px dashed rgba(15, 23, 42, 0.16);
      border-radius: 14px;
      padding: 12px;
      background: rgba(255,255,255,0.55);
      color: #475569;
      font-size: 14px;
    }
    .results {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      padding: 18px;
    }
    .result-card {
      display: block;
      text-decoration: none;
      color: inherit;
      border: 1px solid rgba(15, 23, 42, 0.10);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.92));
      padding: 14px;
      transition: transform 140ms ease, box-shadow 140ms ease, border-color 140ms ease;
    }
    .result-card:hover {
      transform: translateY(-2px);
      border-color: rgba(15, 23, 42, 0.18);
      box-shadow: 0 12px 22px rgba(15, 23, 42, 0.07);
    }
    .result-brand {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--brand);
      margin-bottom: 8px;
    }
    .result-name {
      font-size: 15px;
      line-height: 1.35;
      margin-bottom: 8px;
    }
    .result-category {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 10px;
    }
    .result-meta {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      font-size: 12px;
      color: var(--muted);
    }
    .result-price { color: var(--ink); font-weight: 700; font-size: 13px; }
    .pager {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 0 18px 18px;
    }
    .pager-info { color: var(--muted); font-size: 13px; }
    .pager-actions { display: flex; gap: 8px; }
    .pager-btn {
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.9);
      color: #0f172a;
      border-radius: 999px;
      padding: 9px 12px;
      text-decoration: none;
      font-size: 13px;
    }
    .pager-btn[aria-disabled="true"] {
      pointer-events: none;
      opacity: 0.45;
    }
    @media (max-width: 760px) {
      .topbar { border-radius: 18px; }
      .results { grid-template-columns: 1fr; }
      .pager { flex-direction: column; align-items: flex-start; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <a class="logo" href="/">dimi</a>
      <form class="search-form" action="/search" method="get" role="search">
        <input id="search-input" class="search-input" type="search" name="q" minlength="3" required placeholder="Search products, brands, categories" />
        <button class="search-submit" type="submit">Search</button>
      </form>
      <div class="top-actions">
        <a class="chip" href="/">Offers</a>
        <a class="chip" href="#">Account</a>
      </div>
    </div>

    <section class="panel">
      <div class="panel-head">
        <h1 id="search-title">Search results</h1>
        <div class="panel-sub" id="search-sub">Enter a search to browse products.</div>
      </div>
      <div class="status" id="search-status">Loading search results...</div>
      <div class="results" id="search-results" hidden></div>
      <div class="pager" id="search-pager" hidden>
        <div class="pager-info" id="search-pager-info"></div>
        <div class="pager-actions">
          <a class="pager-btn" id="prev-page" href="#" aria-disabled="true">Previous</a>
          <a class="pager-btn" id="next-page" href="#" aria-disabled="true">Next</a>
        </div>
      </div>
    </section>
  </div>

  <script>
    (function () {
      var params = new URLSearchParams(window.location.search);
      var query = (params.get("q") || "").trim();
      var pageRaw = params.get("page") || "1";
      var page = parseInt(pageRaw, 10);
      if (!Number.isFinite(page) || page < 1) page = 1;

      var inputEl = document.getElementById("search-input");
      var titleEl = document.getElementById("search-title");
      var subEl = document.getElementById("search-sub");
      var statusEl = document.getElementById("search-status");
      var resultsEl = document.getElementById("search-results");
      var pagerEl = document.getElementById("search-pager");
      var pagerInfoEl = document.getElementById("search-pager-info");
      var prevEl = document.getElementById("prev-page");
      var nextEl = document.getElementById("next-page");

      if (inputEl) inputEl.value = query;

      function escapeHtml(s) {
        return String(s == null ? "" : s).replace(/[&<>\"']/g, function (ch) {
          return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" })[ch];
        });
      }

      function formatPrice(item) {
        if (typeof item.price_eur !== "number" || Number.isNaN(item.price_eur)) return "Price unavailable";
        try {
          return new Intl.NumberFormat("de-DE", {
            style: "currency",
            currency: item.currency || "EUR",
            minimumFractionDigits: 2
          }).format(item.price_eur);
        } catch (_) {
          return item.price_eur.toFixed(2) + " " + (item.currency || "EUR");
        }
      }

      function ratingText(item) {
        if (typeof item.rating_value === "number" && item.rating_value > 0) {
          var t = "★ " + item.rating_value.toFixed(1);
          if (typeof item.rating_count === "number" && item.rating_count > 0) t += " (" + item.rating_count + ")";
          return t;
        }
        if (typeof item.rating_count === "number" && item.rating_count > 0) return item.rating_count + " reviews";
        return "New";
      }

      function renderCard(item) {
        var href = item.product_path || ("/product/" + encodeURIComponent(item.gtin || item.id || ""));
        return '' +
          '<a class="result-card" href="' + escapeHtml(href) + '">' +
            '<div class="result-brand">' + escapeHtml(item.brand || "Unknown brand") + '</div>' +
            '<div class="result-name">' + escapeHtml(item.name || "Product") + '</div>' +
            '<div class="result-category">' + escapeHtml(item.category_path || "") + '</div>' +
            '<div class="result-meta">' +
              '<span class="result-price">' + escapeHtml(formatPrice(item)) + '</span>' +
              '<span>' + escapeHtml(ratingText(item)) + '</span>' +
            '</div>' +
          '</a>';
      }

      function pageHref(targetPage) {
        var p = new URLSearchParams(window.location.search);
        p.set("q", query);
        p.set("page", String(targetPage));
        return "/search?" + p.toString();
      }

      if (!query) {
        statusEl.textContent = "Enter at least 3 characters to search.";
        return;
      }

      titleEl.textContent = 'Search results for "' + query + '"';
      subEl.textContent = "Searching product names, brands, and categories.";

      try {
        var inlineError = {{ if .search_error }}{{ printf "%q" .search_error }}{{ else }}""{{ end }};
        if (inlineError) throw new Error(inlineError);
        var data = {{ .search_data_json }};
        var items = Array.isArray(data && data.items) ? data.items : [];
        if (items.length > 0) {
          resultsEl.innerHTML = items.map(renderCard).join("");
          resultsEl.hidden = false;
        } else {
          resultsEl.innerHTML = "";
          resultsEl.hidden = true;
        }

        statusEl.textContent = items.length > 0
          ? ("Showing " + data.returned + " of " + data.total + " results.")
          : "No products found for this search.";

        var maxPage = (typeof data.max_page === "number") ? data.max_page : (data.total_pages || 0);
        var minPage = (typeof data.min_page === "number") ? data.min_page : 1;
        var currentPage = (typeof data.page === "number") ? data.page : page;
        pagerInfoEl.textContent = maxPage > 0
          ? ("Page " + currentPage + " of " + maxPage)
          : "No pages";
        pagerEl.hidden = false;

        if (currentPage > minPage) {
          prevEl.href = pageHref(currentPage - 1);
          prevEl.setAttribute("aria-disabled", "false");
        } else {
          prevEl.href = "#";
          prevEl.setAttribute("aria-disabled", "true");
        }
        if (maxPage > 0 && currentPage < maxPage) {
          nextEl.href = pageHref(currentPage + 1);
          nextEl.setAttribute("aria-disabled", "false");
        } else {
          nextEl.href = "#";
          nextEl.setAttribute("aria-disabled", "true");
        }
      } catch (err) {
        statusEl.textContent = (err && err.message) ? err.message : "Could not load search results right now.";
        resultsEl.hidden = true;
        pagerEl.hidden = true;
      }
    })();
  </script>
</body>
</html>`))

func getString(row map[string]any, key string) string {
	v, ok := row[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	case float64, float32, int64, int32, int, uint64, uint32, uint:
		return fmt.Sprint(t)
	default:
		return fmt.Sprint(t)
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
