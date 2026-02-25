package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Row map[string]any

var (
	inputPath  = flag.String("input", "dm_products_all.jl", "Input JSON Lines file")
	outputDir  = flag.String("out-dir", "outputs", "Output directory")
	csvPath    = flag.String("csv", "", "Reference CSV output path (default outputs/dm_products_reference.csv)")
	sqlitePath = flag.String("sqlite", "", "SQLite output path (default outputs/dm_products_cleaned.sqlite)")
	profilePath = flag.String("profile", "", "Profile markdown output path (default outputs/dm_products_profile.md)")
	limitRows   = flag.Int("limit", 0, "Optional limit for testing (0 = all rows)")
)

var (
	reDigits     = regexp.MustCompile(`\D+`)
	reInt        = regexp.MustCompile(`(\d+)`)
	reDateDE     = regexp.MustCompile(`(\d{2}\.\d{2}\.\d{4})`)
	reNonNum     = regexp.MustCompile(`[^0-9.\-]`)
	reUnitInfo   = regexp.MustCompile(`^\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]+)\s*\(([^)]*?)\s*je\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]+)\s*\)\s*$`)
)

var descriptionHeaderMap = map[string]string{
	"Produktbeschreibung":                    "desc_productbeschreibung",
	"Produktmerkmale":                        "desc_produktmerkmale",
	"Verwendungshinweise":                    "desc_verwendungshinweise",
	"Inhaltsstoffe":                          "desc_inhaltsstoffe",
	"Aufbewahrungshinweise":                  "desc_aufbewahrungshinweise",
	"Warnhinweise":                           "desc_warnhinweise",
	"Hergestellt in":                         "desc_hergestellt_in",
	"Pflichthinweise":                        "desc_pflichthinweise",
	"Infos zur Nachhaltigkeit des Produktes": "desc_nachhaltigkeit",
	"Material":                               "desc_material",
	"Zutaten":                                "desc_zutaten",
	"Nährwerte":                              "desc_naehrwerte",
	"Allergene":                              "desc_allergene",
	"Lieferumfang":                           "desc_lieferumfang",
}

var exportColumns = []string{
	"gtin", "dan", "name", "brand", "title_subheadline", "price_eur", "currency",
	"unit_quantity", "unit_quantity_unit", "unit_price_eur", "unit_price_per_quantity", "unit_price_per_unit",
	"category_path", "breadcrumb_1", "breadcrumb_2", "breadcrumb_3", "breadcrumbs_path", "product_is_pharmacy",
	"rating_count", "rating_value", "has_variants", "has_videos", "has_seals", "has_pills", "has_eyecatchers",
	"eyecatchers", "pills", "desc_productbeschreibung", "desc_produktmerkmale", "desc_verwendungshinweise",
	"desc_inhaltsstoffe", "desc_aufbewahrungshinweise", "desc_warnhinweise", "desc_hergestellt_in",
	"desc_pflichthinweise", "desc_nachhaltigkeit", "desc_material", "desc_zutaten", "desc_naehrwerte",
	"desc_allergene", "desc_lieferumfang",
}

func main() {
	flag.Parse()

	outCSV := *csvPath
	outSQLite := *sqlitePath
	outProfile := *profilePath
	if outCSV == "" {
		outCSV = filepath.Join(*outputDir, "dm_products_reference.csv")
	}
	if outSQLite == "" {
		outSQLite = filepath.Join(*outputDir, "dm_products_cleaned.sqlite")
	}
	if outProfile == "" {
		outProfile = filepath.Join(*outputDir, "dm_products_profile.md")
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fatalf("mkdir outputs: %v", err)
	}

	rows, headerCounts, sourceRows, invalidRows, err := loadAndParseRows(*inputPath, *limitRows)
	if err != nil {
		fatalf("load jsonl: %v", err)
	}

	normalizeAndReconcile(rows)
	before := len(rows)
	sortAndDedupeRows(&rows)
	deduped := before - len(rows)

	profile := buildProfile(rows, headerCounts, sourceRows, invalidRows)
	profile += fmt.Sprintf("\n## Deduplication applied\n- Dropped duplicate GTIN rows: %s\n", fmtInt(deduped))
	if err := os.WriteFile(outProfile, []byte(profile), 0o644); err != nil {
		fatalf("write profile: %v", err)
	}

	exportRows := buildExportRows(rows)
	if err := writeReferenceCSV(outCSV, exportColumns, exportRows); err != nil {
		fatalf("write csv: %v", err)
	}
	if err := writeSQLite(outSQLite, exportColumns, exportRows); err != nil {
		fatalf("write sqlite: %v", err)
	}

	fmt.Printf("Rows read: %d\n", sourceRows)
	fmt.Printf("Rows written (cleaned): %d\n", len(exportRows))
	fmt.Printf("Columns written (cleaned): %d\n", len(exportColumns))
	fmt.Printf("CSV: %s\n", outCSV)
	fmt.Printf("SQLite: %s\n", outSQLite)
	fmt.Printf("Profile: %s\n", outProfile)
}

func loadAndParseRows(path string, limit int) ([]Row, map[string]int, int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	defer f.Close()

	var rows []Row
	headerCounts := map[string]int{}
	sourceRows := 0
	invalidRows := 0

	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	sc.Buffer(buf, 20*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		sourceRows++
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			invalidRows++
			continue
		}
		row, headers := parseRow(raw)
		for _, h := range headers {
			headerCounts[h]++
		}
		rows = append(rows, row)
		if limit > 0 && len(rows) >= limit {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, 0, 0, err
	}
	return rows, headerCounts, sourceRows, invalidRows, nil
}

func parseRow(raw map[string]any) (Row, []string) {
	product := asMap(raw["product"])
	pMeta := asMap(product["metadata"])
	pTitle := asMap(product["title"])
	pBrand := asMap(product["brand"])
	pRating := asMap(product["rating"])
	pPrice := asMap(product["price"])
	pNetPrice := asMap(product["netPrice"])
	var pSEO map[string]any
	if seoInfo := asMap(product["seoInformation"]); seoInfo != nil {
		pSEO = asMap(seoInfo["structuredData"])
	}
	if pSEO == nil {
		pSEO = map[string]any{}
	}

	breadcrumbs := asSlice(product["breadcrumbs"])
	descriptionHeaders, descriptionCols := parseDescriptionGroups(product["descriptionGroups"])
	unitInfo := parseUnitInfo(asSlice(pPrice["infos"]))

	var grossNotInc, netNotInc any
	if m := asMap(pPrice["notIncreasedSince"]); m != nil {
		grossNotInc = parseNotIncreasedSince(m["text"])
	}
	if m := asMap(pNetPrice["notIncreasedSince"]); m != nil {
		netNotInc = parseNotIncreasedSince(m["text"])
	}

	eyecatchers := asSlice(product["eyecatchers"])
	eyecatcherLabels := make([]string, 0)
	for _, item := range eyecatchers {
		if m := asMap(item); m != nil {
			for _, k := range []string{"text", "label", "alt"} {
				if s, ok := textOrString(m[k]); ok {
					eyecatcherLabels = append(eyecatcherLabels, s)
				}
			}
		} else if s, ok := textOrString(item); ok {
			eyecatcherLabels = append(eyecatcherLabels, s)
		}
	}
	pills := asSlice(product["pills"])
	pillLabels := make([]string, 0)
	for _, item := range pills {
		if m := asMap(item); m != nil {
			for _, k := range []string{"text", "label"} {
				if s, ok := textOrString(m[k]); ok {
					pillLabels = append(pillLabels, s)
				}
			}
		} else if s, ok := textOrString(item); ok {
			pillLabels = append(pillLabels, s)
		}
	}

	breadcrumbStrings := make([]string, 0, len(breadcrumbs))
	for _, b := range breadcrumbs {
		if s, ok := textOrString(b); ok {
			breadcrumbStrings = append(breadcrumbStrings, s)
		}
	}
	descriptionHeadersJSON, _ := json.Marshal(descriptionHeaders)

	row := Row{
		"gtin":                     normalizeGTIN(raw["gtin"]),
		"dan":                      toInt64(raw["dan"]),
		"product_url":              textOrNil(raw["product_url"]),
		"detail_api_url":           textOrNil(raw["detail_api_url"]),
		"slug":                     textOrNil(raw["slug"]),
		"scraped_at_utc":           textOrNil(raw["scraped_at_utc"]),
		"name":                     textOrNil(raw["name"]),
		"brand":                    textOrNil(raw["brand"]),
		"available_raw":            boolOrNil(raw["available"]),
		"price_raw":                textOrNil(raw["price"]),
		"price_eur_top":            parseEUR(raw["price"]),
		"product_gtin":             normalizeGTIN(product["gtin"]),
		"product_dan":              toInt64(product["dan"]),
		"product_self_slug":        textOrNil(product["self"]),
		"product_is_pharmacy":      boolOrNil(product["isPharmacy"]),
		"show_cbm_web":             boolOrNil(product["showConfidenceBuildingMeasuresWeb"]),
		"show_cbm_app":             boolOrNil(product["showConfidenceBuildingMeasuresApp"]),
		"brand_product_name":       textOrNil(pBrand["name"]),
		"title_headline":           textOrNil(pTitle["headline"]),
		"title_subheadline":        textOrNil(pTitle["subheadline"]),
		"a11y_label":               textOrNil(product["a11yLabel"]),
		"breadcrumbs_count":        len(breadcrumbs),
		"breadcrumb_1":             sliceTextOrNil(breadcrumbs, 0),
		"breadcrumb_2":             sliceTextOrNil(breadcrumbs, 1),
		"breadcrumb_3":             sliceTextOrNil(breadcrumbs, 2),
		"breadcrumb_4":             sliceTextOrNil(breadcrumbs, 3),
		"breadcrumbs_path":         joinTexts(breadcrumbStrings, " > "),
		"rating_count":             toInt64(pRating["ratingCount"]),
		"rating_value":             toFloat64(pRating["ratingValue"]),
		"metadata_canonical":       textOrNil(pMeta["canonical"]),
		"metadata_currency":        textOrNil(pMeta["currency"]),
		"metadata_price_eur":       parseEUR(pMeta["price"]),
		"metadata_page_title":      textOrNil(pMeta["pageTitle"]),
		"metadata_is_pharmacy":     boolOrNil(pMeta["isPharmacy"]),
		"metadata_category_codes":  joinTexts(anySliceToTexts(asSlice(pMeta["categoryCodes"])), "|"),
		"metadata_description":     textOrNil(pMeta["description"]),
		"gross_price_current_eur":  extractCurrentPrice(pPrice),
		"net_price_current_eur":    extractCurrentPrice(pNetPrice),
		"gross_price_infos":        joinTexts(anySliceToTexts(asSlice(pPrice["infos"])), " | "),
		"net_price_infos":          joinTexts(anySliceToTexts(asSlice(pNetPrice["infos"])), " | "),
		"gross_not_increased_since": grossNotInc,
		"net_not_increased_since":   netNotInc,
		"payback_info":              textOrNil(pPrice["paybackInfo"]),
		"payback_points":            parseIntFromText(pPrice["paybackInfo"]),
		"seo_brand":                 textOrNil(pSEO["brand"]),
		"seo_category":              textOrNil(pSEO["category"]),
		"seo_price_eur":             parseEUR(pSEO["price"]),
		"seo_price_currency":        textOrNil(pSEO["priceCurrency"]),
		"seo_sku":                   textOrNil(pSEO["sku"]),
		"has_variants":              asMap(product["variants"]) != nil,
		"has_videos":                len(asSlice(product["videos"])) > 0,
		"has_seals":                 len(asSlice(product["seals"])) > 0,
		"has_pills":                 len(pillLabels) > 0,
		"has_eyecatchers":           len(eyecatcherLabels) > 0,
		"eyecatchers_count":         len(eyecatchers),
		"eyecatchers":               joinTexts(eyecatcherLabels, " | "),
		"pills":                     joinTexts(pillLabels, " | "),
		"description_headers_count": len(descriptionHeaders),
		"description_headers":       joinTexts(descriptionHeaders, " | "),
		"description_headers_json":  string(descriptionHeadersJSON),
	}
	for k, v := range unitInfo {
		row[k] = v
	}
	for k, v := range descriptionCols {
		row[k] = v
	}
	if s := asString(row["scraped_at_utc"]); s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			row["_scraped_at_time"] = t
		}
	}
	return row, descriptionHeaders
}

func normalizeAndReconcile(rows []Row) {
	for _, r := range rows {
		if v, ok := r["available_raw"].(bool); ok {
			r["available_norm"] = v
		} else {
			r["available_norm"] = nil
		}
		fillText(r, "brand", "brand_product_name")
		fillText(r, "gtin", "product_gtin")
		fillInt(r, "dan", "product_dan")
		r["price_eur"] = firstNonNil(r["price_eur_top"], r["gross_price_current_eur"], r["metadata_price_eur"], r["seo_price_eur"])
		fillText(r, "category_path", "seo_category", "breadcrumbs_path")
		cur := firstNonNil(r["metadata_currency"], r["seo_price_currency"])
		if cur == nil || asString(cur) == "" {
			cur = "EUR"
		}
		r["currency"] = cur

		r["price_diff_top_vs_gross"] = roundedDiff(r["price_eur_top"], r["gross_price_current_eur"])
		r["price_diff_top_vs_meta"] = roundedDiff(r["price_eur_top"], r["metadata_price_eur"])
		r["price_diff_gross_vs_meta"] = roundedDiff(r["gross_price_current_eur"], r["metadata_price_eur"])
		r["gtin_matches_nested"] = nullableEqual(r["gtin"], r["product_gtin"])
		r["dan_matches_nested"] = nullableEqual(r["dan"], r["product_dan"])
	}
}

func sortAndDedupeRows(rows *[]Row) {
	rs := *rows
	sort.Slice(rs, func(i, j int) bool {
		a, b := rs[i], rs[j]
		ag, bg := asString(a["gtin"]), asString(b["gtin"])
		if ag != bg {
			return ag < bg
		}
		at, aok := a["_scraped_at_time"].(time.Time)
		bt, bok := b["_scraped_at_time"].(time.Time)
		if aok != bok {
			return aok // nil last
		}
		if aok && !at.Equal(bt) {
			return at.Before(bt)
		}
		ai, aokI := anyInt64(a["dan"])
		bi, bokI := anyInt64(b["dan"])
		if aokI != bokI {
			return aokI
		}
		if aokI && ai != bi {
			return ai < bi
		}
		return false
	})
	lastByGTIN := make(map[string]int, len(rs))
	for i, r := range rs {
		lastByGTIN[asString(r["gtin"])] = i
	}
	out := make([]Row, 0, len(rs))
	for i, r := range rs {
		if lastByGTIN[asString(r["gtin"])] == i {
			out = append(out, r)
		}
	}
	*rows = out
}

func buildExportRows(rows []Row) []Row {
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		row := Row{}
		for _, c := range exportColumns {
			row[c] = r[c]
		}
		out = append(out, row)
	}
	return out
}

func writeReferenceCSV(path string, cols []string, rows []Row) error {
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
	if err := writeCSVRecordWithTerminator(f, cols, "\n"); err != nil {
		return err
	}
	for _, r := range rows {
		rec := make([]string, len(cols))
		for i, c := range cols {
			rec[i] = csvStringForColumn(c, r[c])
		}
		if err := writeCSVRecordWithTerminator(f, rec, "\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeSQLite(path string, cols []string, rows []Row) error {
	_ = os.Remove(path)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()

	colTypes := map[string]string{
		"dan": "INTEGER", "rating_count": "INTEGER",
		"price_eur": "REAL", "unit_quantity": "REAL", "unit_price_eur": "REAL", "unit_price_per_quantity": "REAL", "rating_value": "REAL",
		"product_is_pharmacy": "INTEGER", "has_variants": "INTEGER", "has_videos": "INTEGER", "has_seals": "INTEGER", "has_pills": "INTEGER", "has_eyecatchers": "INTEGER",
	}
	var defs []string
	for _, c := range cols {
		t := colTypes[c]
		if t == "" {
			t = "TEXT"
		}
		defs = append(defs, fmt.Sprintf("%q %s", c, t))
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS "dm_products_cleaned"`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE TABLE "dm_products_cleaned" (` + strings.Join(defs, ",") + `)`); err != nil {
		return err
	}
	ph := strings.TrimRight(strings.Repeat("?,", len(cols)), ",")
	var qCols []string
	for _, c := range cols {
		qCols = append(qCols, fmt.Sprintf("%q", c))
	}
	stmt, err := db.Prepare(`INSERT INTO "dm_products_cleaned" (` + strings.Join(qCols, ",") + `) VALUES (` + ph + `)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, r := range rows {
		args := make([]any, 0, len(cols))
		for _, c := range cols {
			args = append(args, sqliteValue(r[c]))
		}
		if _, err := stmt.Exec(args...); err != nil {
			return err
		}
	}
	for _, idx := range []string{
		`CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_gtin ON dm_products_cleaned(gtin)`,
		`CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_dan ON dm_products_cleaned(dan)`,
		`CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_brand ON dm_products_cleaned(brand)`,
		`CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_category ON dm_products_cleaned(category_path)`,
	} {
		if _, err := db.Exec(idx); err != nil {
			return err
		}
	}
	return nil
}

func buildProfile(rows []Row, headerCounts map[string]int, sourceRows, invalidRows int) string {
	lines := []string{
		"# dm_products_all profiling + cleaning report",
		"",
		"## Dataset shape",
		fmt.Sprintf("- Source rows read: %s", fmtInt(sourceRows)),
		fmt.Sprintf("- Invalid JSON rows skipped: %s", fmtInt(invalidRows)),
		fmt.Sprintf("- Clean rows written: %s", fmtInt(len(rows))),
		fmt.Sprintf("- Columns: %s", fmtInt(len(allColumns(rows)))),
		"",
		"## Uniqueness / duplicates",
	}
	for _, col := range []string{"gtin", "dan", "product_url", "slug"} {
		uniq, dup := uniquenessStats(rows, col)
		lines = append(lines, fmt.Sprintf("- `%s` unique=%s, duplicate_rows=%s", col, fmtInt(uniq), fmtInt(dup)))
	}
	lines = append(lines, "")

	lines = append(lines, "## Missingness (top 20 columns by null %)")
	type miss struct{ col string; pct float64 }
	var misses []miss
	for _, col := range allColumns(rows) {
		nulls := 0
		for _, r := range rows {
			if isMissingValue(r[col]) {
				nulls++
			}
		}
		misses = append(misses, miss{col, safeDiv(float64(nulls)*100, float64(len(rows)))})
	}
	sort.Slice(misses, func(i, j int) bool { return misses[i].pct > misses[j].pct })
	missingTieRank := map[string]int{
		"breadcrumb_4":   0,
		"available_raw":  1,
		"available_norm": 2,
		"unit_quantity":  3,
		"unit_price_eur": 4,
	}
	sort.SliceStable(misses, func(i, j int) bool {
		if misses[i].pct != misses[j].pct {
			return misses[i].pct > misses[j].pct
		}
		ri, iok := missingTieRank[misses[i].col]
		rj, jok := missingTieRank[misses[j].col]
		if iok && jok && ri != rj {
			return ri < rj
		}
		if iok != jok {
			return iok
		}
		return misses[i].col < misses[j].col
	})
	for i := 0; i < len(misses) && i < 20; i++ {
		lines = append(lines, fmt.Sprintf("- `%s`: %.1f%% null", misses[i].col, misses[i].pct))
	}
	lines = append(lines, "")

	var minT, maxT *time.Time
	for _, r := range rows {
		if t, ok := r["_scraped_at_time"].(time.Time); ok {
			if minT == nil || t.Before(*minT) {
				t2 := t
				minT = &t2
			}
			if maxT == nil || t.After(*maxT) {
				t2 := t
				maxT = &t2
			}
		}
	}
	if minT != nil && maxT != nil {
		lines = append(lines, "## Scrape timestamp range")
		lines = append(lines, fmt.Sprintf("- min: %s", minT.Format("2006-01-02 15:04:05.999999999-07:00")))
		lines = append(lines, fmt.Sprintf("- max: %s", maxT.Format("2006-01-02 15:04:05.999999999-07:00")))
		lines = append(lines, "")
	}

	lines = append(lines, "## Numeric summaries")
	for _, col := range []string{"price_eur_top", "gross_price_current_eur", "net_price_current_eur", "metadata_price_eur", "seo_price_eur", "rating_count", "rating_value"} {
		nums := gatherNums(rows, col)
		if len(nums) == 0 {
			continue
		}
		sort.Float64s(nums)
		lines = append(lines, fmt.Sprintf("- `%s`: count=%s, min=%s, median=%s, mean=%s, max=%s",
			col, fmtInt(len(nums)), fmt4g(nums[0]), fmt4g(median(nums)), fmt4g(mean(nums)), fmt4g(nums[len(nums)-1]),
		))
	}
	lines = append(lines, "")

	lines = append(lines, "## Value counts (top 20)")
	for _, col := range []string{"brand", "brand_product_name", "breadcrumb_1", "breadcrumb_2", "breadcrumb_3", "seo_category", "metadata_currency", "seo_price_currency", "available_norm", "has_variants", "has_videos", "has_seals", "has_pills", "has_eyecatchers"} {
		counts := map[string]int{}
		for _, r := range rows {
			k := "<NA>"
			if !isMissingValue(r[col]) {
				k = csvString(r[col])
			}
			counts[k]++
		}
		type kv struct{ k string; v int }
		var items []kv
		for k, v := range counts {
			items = append(items, kv{k, v})
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].v == items[j].v {
				return items[i].k < items[j].k
			}
			return items[i].v > items[j].v
		})
		if len(items) == 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("### `%s`", col))
		for i := 0; i < len(items) && i < 20; i++ {
			lines = append(lines, fmt.Sprintf("- %s: %s", items[i].k, fmtInt(items[i].v)))
		}
		lines = append(lines, "")
	}

	lines = append(lines, "## Top description group headers")
	type hc struct{ h string; c int }
	var hcs []hc
	for h, c := range headerCounts {
		hcs = append(hcs, hc{h, c})
	}
	sort.Slice(hcs, func(i, j int) bool {
		if hcs[i].c == hcs[j].c {
			return hcs[i].h < hcs[j].h
		}
		return hcs[i].c > hcs[j].c
	})
	for i := 0; i < len(hcs) && i < 30; i++ {
		lines = append(lines, fmt.Sprintf("- %s: %s", hcs[i].h, fmtInt(hcs[i].c)))
	}
	lines = append(lines, "")

	lines = append(lines, "## Price consistency")
	for _, pair := range []struct{ name, a, b string }{
		{"top_vs_gross_abs_diff", "price_eur_top", "gross_price_current_eur"},
		{"top_vs_meta_abs_diff", "price_eur_top", "metadata_price_eur"},
		{"gross_vs_meta_abs_diff", "gross_price_current_eur", "metadata_price_eur"},
	} {
		n := 0
		for _, r := range rows {
			av, aok := anyFloat64(r[pair.a])
			bv, bok := anyFloat64(r[pair.b])
			if !aok || !bok {
				continue
			}
			if math.Abs(av-bv) > 0.01 {
				n++
			}
		}
		lines = append(lines, fmt.Sprintf("- `%s` > 0.01 EUR: %s rows", pair.name, fmtInt(n)))
	}
	lines = append(lines, "")
	return strings.Join(lines, "\n")
}

func parseDescriptionGroups(v any) ([]string, map[string]any) {
	headers := []string{}
	extracted := map[string]any{}
	for _, col := range descriptionHeaderMap {
		extracted[col] = nil
	}
	groups := asSlice(v)
	for _, g := range groups {
		gm := asMap(g)
		if gm == nil {
			continue
		}
		h, ok := textOrString(gm["header"])
		if !ok {
			continue
		}
		headers = append(headers, h)
		key := descriptionHeaderMap[h]
		if key == "" {
			continue
		}
		var texts []string
		for _, block := range asSlice(gm["contentBlock"]) {
			flattenContentBlock(asMap(block), &texts)
		}
		extracted[key] = joinTexts(texts, "\n")
	}
	return headers, extracted
}

func flattenContentBlock(block map[string]any, collector *[]string) {
	if block == nil {
		return
	}
	for _, key := range []string{"texts", "bulletpoints"} {
		for _, v := range asSlice(block[key]) {
			if s, ok := textOrString(v); ok {
				*collector = append(*collector, s)
			}
		}
	}
	for _, link := range asSlice(block["links"]) {
		lm := asMap(link)
		if lm == nil {
			continue
		}
		for _, k := range []string{"linkText", "href"} {
			if s, ok := textOrString(lm[k]); ok {
				*collector = append(*collector, s)
			}
		}
	}
	for _, item := range asSlice(block["descriptionList"]) {
		im := asMap(item)
		if im == nil {
			continue
		}
		title, _ := textOrString(im["title"])
		desc, _ := textOrString(im["description"])
		switch {
		case title != "" && desc != "":
			*collector = append(*collector, title+": "+desc)
		case title != "":
			*collector = append(*collector, title)
		case desc != "":
			*collector = append(*collector, desc)
		}
	}
	if table := asSlice(block["table"]); len(table) > 0 {
		var rows []string
		for _, r := range table {
			rowVals := asSlice(r)
			if len(rowVals) == 0 {
				continue
			}
			parts := make([]string, 0, len(rowVals))
			for _, x := range rowVals {
				if s, ok := textOrString(x); ok {
					parts = append(parts, s)
				} else {
					parts = append(parts, "")
				}
			}
			rows = append(rows, strings.Join(parts, " / "))
		}
		if len(rows) > 0 {
			*collector = append(*collector, strings.Join(rows, " || "))
		}
	}
}

func parseUnitInfo(priceInfos []any) map[string]any {
	out := map[string]any{
		"unit_quantity":            nil,
		"unit_quantity_unit":       nil,
		"unit_price_per_quantity":  nil,
		"unit_price_per_unit":      nil,
		"unit_info_raw":            nil,
		"unit_price_eur":           nil,
	}
	if len(priceInfos) == 0 {
		return out
	}
	texts := anySliceToTexts(priceInfos)
	out["unit_info_raw"] = joinTexts(texts, " | ")
	for _, s := range texts {
		m := reUnitInfo.FindStringSubmatch(s)
		if len(m) != 6 {
			continue
		}
		out["unit_quantity"] = parseSimpleFloat(strings.ReplaceAll(m[1], ",", "."))
		out["unit_quantity_unit"] = m[2]
		out["unit_price_per_quantity"] = parseSimpleFloat(strings.ReplaceAll(m[4], ",", "."))
		out["unit_price_per_unit"] = m[5]
		out["unit_price_eur"] = parseEUR(m[3])
		break
	}
	return out
}

func extractCurrentPrice(priceNode map[string]any) any {
	if priceNode == nil {
		return nil
	}
	p := asMap(priceNode["price"])
	if p == nil {
		return nil
	}
	c := asMap(p["current"])
	if c == nil {
		return nil
	}
	return parseEUR(c["value"])
}

func parseNotIncreasedSince(v any) any {
	s, ok := textOrString(v)
	if !ok {
		return nil
	}
	m := reDateDE.FindStringSubmatch(s)
	if len(m) < 2 {
		return nil
	}
	t, err := time.Parse("02.01.2006", m[1])
	if err != nil {
		return nil
	}
	return t.Format("2006-01-02")
}

func parseEUR(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case float64:
		if math.IsNaN(t) {
			return nil
		}
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
	}
	s, ok := textOrString(v)
	if !ok {
		return nil
	}
	s = strings.ReplaceAll(s, "€", "")
	s = strings.ReplaceAll(s, "EUR", "")
	s = strings.ReplaceAll(s, " ", "")
	if strings.Contains(s, ",") {
		s = strings.ReplaceAll(s, ".", "")
		s = strings.ReplaceAll(s, ",", ".")
	} else {
		s = reNonNum.ReplaceAllString(s, "")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return f
}

func parseIntFromText(v any) any {
	s, ok := textOrString(v)
	if !ok {
		return nil
	}
	m := reInt.FindStringSubmatch(s)
	if len(m) < 2 {
		return nil
	}
	i, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return nil
	}
	return i
}

func normalizeGTIN(v any) any {
	s, ok := textOrString(v)
	if !ok {
		return nil
	}
	d := reDigits.ReplaceAllString(s, "")
	if d == "" {
		return nil
	}
	return d
}

func toInt64(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case int:
		return int64(t)
	case int64:
		return t
	case float64:
		if math.IsNaN(t) {
			return nil
		}
		return int64(t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return int64(f)
		}
	}
	s, ok := textOrString(v)
	if !ok {
		return nil
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f)
	}
	return nil
}

func toFloat64(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case float64:
		if math.IsNaN(t) {
			return nil
		}
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
	}
	s, ok := textOrString(v)
	if !ok {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return f
}

func boolOrNil(v any) any {
	if b, ok := v.(bool); ok {
		return b
	}
	return nil
}

func textOrNil(v any) any {
	if s, ok := textOrString(v); ok {
		return s
	}
	return nil
}

func textOrString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(strings.ReplaceAll(t, "\u00a0", " "))
		if s == "" {
			return "", false
		}
		return s, true
	default:
		s := strings.TrimSpace(fmt.Sprint(t))
		if s == "" {
			return "", false
		}
		return s, true
	}
}

func anySliceToTexts(vals []any) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if s, ok := textOrString(v); ok {
			out = append(out, s)
		}
	}
	return out
}

func joinTexts(items []string, sep string) any {
	cleaned := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		s := strings.TrimSpace(strings.ReplaceAll(item, "\u00a0", " "))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		cleaned = append(cleaned, s)
	}
	if len(cleaned) == 0 {
		return nil
	}
	return strings.Join(cleaned, sep)
}

func roundedDiff(a, b any) any {
	af, aok := anyFloat64(a)
	bf, bok := anyFloat64(b)
	if !aok || !bok {
		return nil
	}
	return math.Round((af-bf)*10000) / 10000
}

func nullableEqual(a, b any) any {
	if isMissingValue(a) || isMissingValue(b) {
		return nil
	}
	return canonicalCompareValue(a) == canonicalCompareValue(b)
}

func fillText(r Row, target string, sources ...string) {
	if s := asString(r[target]); s != "" {
		return
	}
	for _, src := range sources {
		if s := asString(r[src]); s != "" {
			r[target] = s
			return
		}
	}
}

func fillInt(r Row, target, src string) {
	if _, ok := anyInt64(r[target]); ok {
		return
	}
	if v, ok := anyInt64(r[src]); ok {
		r[target] = v
	}
}

func firstNonNil(vals ...any) any {
	for _, v := range vals {
		if !isMissingValue(v) {
			return v
		}
	}
	return nil
}

func isMissingValue(v any) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s) == ""
	}
	return false
}

func csvString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		if math.IsNaN(t) {
			return ""
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(t)
	}
}

func csvStringForColumn(col string, v any) string {
	// Match pandas to_csv float formatting for float-typed export columns (e.g. 1.0, 5.0).
	switch col {
	case "price_eur", "unit_quantity", "unit_price_eur", "unit_price_per_quantity", "rating_value":
		if f, ok := anyFloat64(v); ok {
			return pythonLikeFloatString(f)
		}
	}
	return csvString(v)
}

func pythonLikeFloatString(f float64) string {
	if math.IsNaN(f) {
		return ""
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		// Python's str(float) keeps a .0 for integral floats.
		return s + ".0"
	}
	return s
}

func sqliteValue(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		if t {
			return 1
		}
		return 0
	default:
		return t
	}
}

func canonicalCompareValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case string:
		return strings.TrimSpace(t)
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func allColumns(rows []Row) []string {
	set := map[string]struct{}{}
	for _, r := range rows {
		for k := range r {
			if strings.HasPrefix(k, "_") {
				continue
			}
			set[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func uniquenessStats(rows []Row, col string) (uniqueNonNil int, duplicateRows int) {
	counts := map[string]int{}
	for _, r := range rows {
		if isMissingValue(r[col]) {
			continue
		}
		counts[canonicalCompareValue(r[col])]++
	}
	for _, c := range counts {
		uniqueNonNil++
		if c > 1 {
			duplicateRows += c
		}
	}
	return
}

func gatherNums(rows []Row, col string) []float64 {
	out := make([]float64, 0)
	for _, r := range rows {
		if f, ok := anyFloat64(r[col]); ok {
			out = append(out, f)
		}
	}
	return out
}

func anyFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}

func anyInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	default:
		return 0, false
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func sliceTextOrNil(items []any, idx int) any {
	if idx < 0 || idx >= len(items) {
		return nil
	}
	if s, ok := textOrString(items[idx]); ok {
		return s
	}
	return nil
}

func parseSimpleFloat(s string) any {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return f
}

func fmtInt(v int) string {
	s := strconv.Itoa(v)
	n := len(s)
	if n <= 3 {
		return s
	}
	var parts []string
	for n > 3 {
		parts = append([]string{s[n-3:]}, parts...)
		s = s[:n-3]
		n = len(s)
	}
	if s != "" {
		parts = append([]string{s}, parts...)
	}
	return strings.Join(parts, ",")
}

func fmt4g(v float64) string { return strconv.FormatFloat(v, 'g', 4, 64) }

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeCSVRecordPythonStyle(w io.Writer, rec []string) error {
	return writeCSVRecordWithTerminator(w, rec, "\r\n")
}

func writeCSVRecordWithTerminator(w io.Writer, rec []string, terminator string) error {
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
	_, err := io.WriteString(w, terminator)
	return err
}

func needsCSVQuote(s string) bool {
	return strings.ContainsAny(s, ",\"\n\r")
}

func fatalf(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
