package main

import (
	"database/sql"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const defaultAddr = "127.0.0.1:18745"
const sitemapProtocolMaxURLs = 50000
const defaultSitemapChunkSize = 10000

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
			"title":         "dimi",
			"sections_html": renderHomeSectionsHTML(payload),
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
			"id":           id,
			"name":         firstNonEmpty(getString(row, "name"), getString(row, "title_headline"), "Product "+id),
			"brand":        firstNonEmpty(getString(row, "brand"), getString(row, "seo_brand"), "Unknown brand"),
			"price":        firstNonEmpty(getString(row, "price_raw"), getString(row, "price_eur"), getString(row, "metadata_price_eur")),
			"category":     firstNonEmpty(getString(row, "category_path"), getString(row, "seo_category")),
			"image":        firstNonEmpty(getString(row, "image"), getString(row, "image_url"), getString(row, "img"), getString(row, "thumbnail")),
			"desc":         firstNonEmpty(getString(row, "desc_productbeschreibung"), getString(row, "metadata_description")),
			"similar_html": renderSimilarCardsHTML(similar),
			"has_similar":  len(similar) > 0,
		}); err != nil {
			log.Printf("template error: %v", err)
		}
	})

	log.Printf("medium-server-2 listening on %s (table=%s id=%s)", *addr, table, *idCol)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
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

func renderHomeSectionsHTML(payload homePayload) template.HTML {
	var b strings.Builder
	for _, section := range payload.Sections {
		items := section.Items
		if items == nil {
			items = []map[string]any{}
		}
		b.WriteString(`<section class="section" data-section-id="`)
		b.WriteString(template.HTMLEscapeString(section.ID))
		b.WriteString(`"><div class="section-head"><div><h2 class="section-title">`)
		b.WriteString(template.HTMLEscapeString(section.Title))
		b.WriteString(`</h2>`)
		if strings.TrimSpace(section.Description) != "" {
			b.WriteString(`<p class="section-desc">`)
			b.WriteString(template.HTMLEscapeString(section.Description))
			b.WriteString(`</p>`)
		}
		b.WriteString(`</div><div class="section-meta">`)
		b.WriteString(fmt.Sprintf("%d items", len(items)))
		b.WriteString(`</div></div><div class="cards">`)
		for _, item := range items {
			b.WriteString(renderHomeCardHTML(item))
		}
		b.WriteString(`</div></section>`)
	}
	return template.HTML(b.String())
}

func renderHomeCardHTML(item map[string]any) string {
	gtin := getString(item, "gtin")
	href := firstNonEmpty(getString(item, "product_path"), "/product/"+gtin)
	brand := firstNonEmpty(getString(item, "brand"), "Unknown brand")
	name := firstNonEmpty(getString(item, "name"), "Product")
	category := getString(item, "category_path")
	price := formatCurrencyFromMap(item)
	rating := formatRatingSummary(item)

	var b strings.Builder
	b.WriteString(`<a class="card" href="`)
	b.WriteString(template.HTMLEscapeString(href))
	b.WriteString(`"><div class="card-brand">`)
	b.WriteString(template.HTMLEscapeString(brand))
	b.WriteString(`</div><div class="card-name">`)
	b.WriteString(template.HTMLEscapeString(name))
	b.WriteString(`</div><div class="card-category">`)
	b.WriteString(template.HTMLEscapeString(category))
	b.WriteString(`</div><div class="card-foot"><span class="price">`)
	b.WriteString(template.HTMLEscapeString(price))
	b.WriteString(`</span><span>`)
	b.WriteString(template.HTMLEscapeString(rating))
	b.WriteString(`</span></div></a>`)
	return b.String()
}

func renderSimilarCardsHTML(items []map[string]any) template.HTML {
	var b strings.Builder
	for _, item := range items {
		gtin := getString(item, "gtin")
		name := firstNonEmpty(getString(item, "name"), "Product")
		brand := firstNonEmpty(getString(item, "brand"), "Unknown brand")
		price := formatCurrencyFromMap(item)
		rating := ""
		if rv, ok := getFloat(item, "rating_value"); ok && rv > 0 {
			rating = fmt.Sprintf("★ %.1f", rv)
		}

		b.WriteString(`<a class="rec-card" href="/product/`)
		b.WriteString(template.URLQueryEscaper(gtin))
		b.WriteString(`"><div class="rec-brand">`)
		b.WriteString(template.HTMLEscapeString(brand))
		b.WriteString(`</div><div class="rec-name">`)
		b.WriteString(template.HTMLEscapeString(name))
		b.WriteString(`</div><div class="rec-meta"><span class="rec-price">`)
		b.WriteString(template.HTMLEscapeString(price))
		b.WriteString(`</span><span>`)
		b.WriteString(template.HTMLEscapeString(rating))
		b.WriteString(`</span></div></a>`)
	}
	return template.HTML(b.String())
}

func formatCurrencyFromMap(item map[string]any) string {
	if price, ok := getFloat(item, "price_eur"); ok {
		currency := firstNonEmpty(getString(item, "currency"), "EUR")
		return fmt.Sprintf("%.2f %s", price, currency)
	}
	return "Price unavailable"
}

func formatRatingSummary(item map[string]any) string {
	if rv, ok := getFloat(item, "rating_value"); ok && rv > 0 {
		if rc, ok2 := getInt(item, "rating_count"); ok2 && rc > 0 {
			return fmt.Sprintf("★ %.1f (%d)", rv, rc)
		}
		return fmt.Sprintf("★ %.1f", rv)
	}
	if rc, ok := getInt(item, "rating_count"); ok && rc > 0 {
		return fmt.Sprintf("%d reviews", rc)
	}
	return "New"
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
    .specs { margin-top: 18px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 18px; font-size: 14px; color: var(--muted); }
    .specs div { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
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
  </style>
</head>
<body>
  <div class="wrap">
    <div class="crumbs">{{ if .category }}{{ .category }}{{ else }}Product details{{ end }}</div>
    <div class="card">
      <div class="media">
        {{ if .image }}
        <img src="{{ .image }}" alt="{{ .name }}" />
        {{ else }}
        <span>No image</span>
        {{ end }}
      </div>
      <div>
        <div class="brand">{{ .brand }}</div>
        <h1>{{ .name }}</h1>
        <div class="price-row">
          <div class="price">{{ if .price }}{{ .price }}{{ else }}Price not available{{ end }}</div>
          <div class="pill">In stock</div>
        </div>
        <div class="meta">
          <span>Product ID: <span>{{ .id }}</span></span>
          {{ if .category }}<span>Category: <span>{{ .category }}</span></span>{{ end }}
        </div>
        <a class="cta" href="#">Add to cart</a>
        <a class="cta-secondary" href="#">Wishlist</a>
        {{ if .desc }}<div class="desc">{{ .desc }}</div>{{ end }}
        <div class="meta">Rendered on the server.</div>
        <div class="specs">
          <div>Shipping: 2-4 days</div>
          <div>Returns: 30 days</div>
          <div>Support: Email & chat</div>
          <div>Secure checkout</div>
        </div>
      </div>
    </div>
    <section class="recs" id="similar-products">
      <h2>Products you may also like</h2>
      <div class="recs-sub">Related suggestions selected on the server.</div>
      {{ if .has_similar }}
      <div class="recs-status">Suggestions ready.</div>
      <div class="recs-grid">{{ .similar_html }}</div>
      {{ else }}
      <div class="recs-status">No suggestions available right now.</div>
      {{ end }}
    </section>
  </div>
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
      <div class="top-actions">
        <a class="chip" href="#">Offers</a>
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
          <div class="status" id="home-status">Collections ready.</div>
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

    <main class="sections" id="sections" aria-live="polite">{{ .sections_html }}</main>
    <div class="footer-note">Curated for everyday shopping across categories customers love.</div>
  </div>

  <script>
    (function () {
      var statusEl = document.getElementById("home-status");
      var sectionsEl = document.getElementById("sections");
      var scrollBtn = document.getElementById("scroll-sections");

      if (scrollBtn && sectionsEl) {
        scrollBtn.addEventListener("click", function () {
          sectionsEl.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      }

      setTimeout(function () {
        if (statusEl) statusEl.hidden = true;
      }, 1200);
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

func getFloat(row map[string]any, key string) (float64, bool) {
	v, ok := row[key]
	if !ok || v == nil {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case int32:
		return float64(t), true
	case uint:
		return float64(t), true
	case uint64:
		return float64(t), true
	case uint32:
		return float64(t), true
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case []byte:
		s := strings.TrimSpace(string(t))
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func getInt(row map[string]any, key string) (int64, bool) {
	v, ok := row[key]
	if !ok || v == nil {
		return 0, false
	}
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case int32:
		return int64(t), true
	case uint:
		return int64(t), true
	case uint64:
		if t > math.MaxInt64 {
			return 0, false
		}
		return int64(t), true
	case uint32:
		return int64(t), true
	case float64:
		return int64(t), true
	case float32:
		return int64(t), true
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case []byte:
		s := strings.TrimSpace(string(t))
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
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
