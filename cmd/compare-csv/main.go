package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type csvTable struct {
	Path    string
	Headers []string
	Rows    []map[string]string
}

type colProfile struct {
	RowCount                int      `json:"row_count"`
	NonEmptyCount           int      `json:"non_empty_count"`
	NullCount               int      `json:"null_count"`
	UniqueNonEmptyCount     int      `json:"unique_non_empty_count"`
	IsUniqueNonEmpty        bool     `json:"is_unique_non_empty"`
	UniquenessRatioNonEmpty float64  `json:"uniqueness_ratio_non_empty"`
	NumericRatio            float64  `json:"numeric_ratio"`
	BoolRatio               float64  `json:"bool_ratio"`
	AvgLenSample            float64  `json:"avg_len_sample"`
	MaxLenSample            float64  `json:"max_len_sample"`
	HeaderTokens            []string `json:"header_tokens"`
}

type configPayload struct {
	ReferenceCSV             string      `json:"reference_csv"`
	CandidateCSV             string      `json:"candidate_csv"`
	SampleSizeMapping        int         `json:"sample_size_mapping,omitempty"`
	ColumnWeighting          interface{} `json:"column_weighting"`
	MissingReferenceColScore float64     `json:"missing_reference_column_score"`
	ExtraCandidatePenalize   bool        `json:"extra_candidate_columns_penalize"`
}

type refProfilePayload struct {
	RowCount      int      `json:"row_count"`
	ColumnCount   int      `json:"column_count"`
	UniqueColumns []string `json:"unique_columns"`
}

type candProfilePayload struct {
	RowCount    int `json:"row_count"`
	ColumnCount int `json:"column_count"`
}

type keyCandidate struct {
	ReferenceColumn      string  `json:"reference_column"`
	CandidateColumn      string  `json:"candidate_column"`
	CompleteSetMatch     bool    `json:"complete_set_match"`
	IntersectionCount    int     `json:"intersection_count"`
	CandidateKeyCoverage float64 `json:"candidate_key_coverage"`
	ReferenceKeyCoverage float64 `json:"reference_key_coverage"`
	HeaderSimilarity     float64 `json:"header_similarity"`
	ReferenceNonEmpty    int     `json:"reference_non_empty_count"`
	CandidateNonEmpty    int     `json:"candidate_non_empty_count"`
	Score                float64 `json:"score"`
}

type keyMatchPayload struct {
	FoundUsableMatch   bool           `json:"found_usable_match"`
	FoundCompleteMatch bool           `json:"found_complete_match"`
	MatchMode          string         `json:"match_mode,omitempty"`
	ReferenceColumn    *string        `json:"reference_column"`
	CandidateColumn    *string        `json:"candidate_column"`
	Reason             string         `json:"reason"`
	Candidates         []keyCandidate `json:"candidates"`
}

type rowAlignmentPayload struct {
	Complete                      bool     `json:"complete"`
	ReferenceKey                  string   `json:"reference_key,omitempty"`
	CandidateKey                  string   `json:"candidate_key,omitempty"`
	MatchedRows                   int      `json:"matched_rows"`
	ReferenceRows                 int      `json:"reference_rows"`
	CandidateRows                 int      `json:"candidate_rows"`
	CoverageReference             float64  `json:"coverage_reference"`
	CoverageCandidate             float64  `json:"coverage_candidate"`
	DuplicateReferenceKeys        int      `json:"duplicate_reference_keys,omitempty"`
	DuplicateCandidateMatches     int      `json:"duplicate_candidate_matches,omitempty"`
	MissingCandidateKeysOrMissing int      `json:"missing_candidate_keys_or_unmatched,omitempty"`
	Pairs                         [][2]int `json:"-"`
}

type mappingPair struct {
	ReferenceColumn   string  `json:"reference_column"`
	CandidateColumn   string  `json:"candidate_column"`
	HeaderSimilarity  float64 `json:"header_similarity"`
	TypeCompatibility float64 `json:"type_compatibility"`
	SampleSimilarity  float64 `json:"sample_similarity"`
	MappingConfidence float64 `json:"mapping_confidence"`
}

type columnMappingPayload struct {
	Mapping              map[string]mappingPair `json:"mapping"`
	ReferenceUnmatched   []string               `json:"reference_unmatched"`
	CandidateUnmatched   []string               `json:"candidate_unmatched"`
	MappingConfidenceAvg float64                `json:"mapping_confidence_avg"`
	PairCandidatesTop    []mappingPair          `json:"pair_candidates_top"`
}

type perColumnScore struct {
	ReferenceColumn   string  `json:"reference_column"`
	CandidateColumn   *string `json:"candidate_column"`
	Similarity        float64 `json:"similarity"`
	Matched           bool    `json:"matched"`
	Reason            string  `json:"reason,omitempty"`
	MappingConfidence float64 `json:"mapping_confidence,omitempty"`
	RowCountScored    int     `json:"row_count_scored,omitempty"`
	HeaderSimilarity  float64 `json:"header_similarity,omitempty"`
	SampleSimilarity  float64 `json:"sample_similarity,omitempty"`
}

type scoresPayload struct {
	DatasetSimilarityEqualWeighted float64          `json:"dataset_similarity_equal_weighted"`
	OverallScoreWithCoverage       float64          `json:"overall_score_with_coverage"`
	MappedReferenceColumns         int              `json:"mapped_reference_columns"`
	ReferenceColumnsTotal          int              `json:"reference_columns_total"`
	PerReferenceColumn             []perColumnScore `json:"per_reference_column"`
}

type summaryPayload struct {
	Status                         string  `json:"status"`
	DatasetSimilarityEqualWeighted float64 `json:"dataset_similarity_equal_weighted"`
	CoverageReference              float64 `json:"coverage_reference"`
	CoverageCandidate              float64 `json:"coverage_candidate"`
	OverallScoreWithCoverage       float64 `json:"overall_score_with_coverage"`
	MatchedRows                    int     `json:"matched_rows"`
	ReferenceRows                  int     `json:"reference_rows"`
	CandidateRows                  int     `json:"candidate_rows"`
	MappedReferenceColumns         int     `json:"mapped_reference_columns"`
	ReferenceColumnsTotal          int     `json:"reference_columns_total"`
	KeyMatchMode                   string  `json:"key_match_mode,omitempty"`
	KeyReferenceColumn             *string `json:"key_reference_column,omitempty"`
	KeyCandidateColumn             *string `json:"key_candidate_column,omitempty"`
}

type reportPayload struct {
	Status           string               `json:"status"`
	Summary          summaryPayload       `json:"summary"`
	Config           configPayload        `json:"config"`
	ReferenceProfile refProfilePayload    `json:"reference_profile"`
	CandidateProfile candProfilePayload   `json:"candidate_profile"`
	RowAlignment     rowAlignmentPayload  `json:"row_alignment"`
	KeyMatch         keyMatchPayload      `json:"key_match"`
	ColumnMapping    columnMappingPayload `json:"column_mapping"`
	Scores           scoresPayload        `json:"scores"`
}

var (
	reNumeric          = regexp.MustCompile(`^[+-]?(?:\d+\.?\d*|\.\d+)$`)
	reToken            = regexp.MustCompile(`[a-z0-9]+`)
	headerTokenAliases = map[string]string{
		"crumb":      "breadcrumb",
		"crumbs":     "breadcrumbs",
		"tree":       "path",
		"details":    "desc",
		"highlights": "eyecatchers",
		"badges":     "pills",
		"reviews":    "rating",
		"score":      "value",
		"qty":        "quantity",
		"pack":       "unit",
		"subline":    "subheadline",
		"amt":        "",
		"code":       "",
		"is":         "has",
		"product":    "",
	}
)

func main() {
	reference := flag.String("reference", "outputs/sample_products_reference.csv", "Reference CSV (ground truth)")
	candidate := flag.String("candidate", "outputs/sample_products_candidate1.csv", "Candidate CSV to evaluate")
	outputJSON := flag.String("output-json", "", "Optional path to write JSON report")
	sampleSizeMapping := flag.Int("sample-size-mapping", 256, "Aligned-row sample size used for column mapping confidence")
	flag.Parse()

	report, err := compareCSVFiles(*reference, *candidate, *sampleSizeMapping)
	if err != nil {
		fmt.Fprintf(os.Stderr, "compare error: %v\n", err)
		os.Exit(1)
	}

	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
		os.Exit(1)
	}

	if *outputJSON != "" {
		if err := os.MkdirAll(filepath.Dir(*outputJSON), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir error: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*outputJSON, append(payload, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Wrote JSON report: %s\n", *outputJSON)
		fmt.Printf("Status: %s\n", report.Status)
		fmt.Printf("Dataset similarity (equal weighted): %.12f\n", report.Scores.DatasetSimilarityEqualWeighted)
		fmt.Printf("Coverage (reference/candidate): %.12f / %.12f\n", report.RowAlignment.CoverageReference, report.RowAlignment.CoverageCandidate)
		fmt.Printf("Overall score with coverage: %.12f\n", report.Scores.OverallScoreWithCoverage)
		return
	}
	fmt.Println(string(payload))
}

func compareCSVFiles(referenceCSV, candidateCSV string, sampleSizeMapping int) (reportPayload, error) {
	if sampleSizeMapping < 0 {
		sampleSizeMapping = 0
	}
	ref, err := loadCSV(referenceCSV)
	if err != nil {
		return reportPayload{}, err
	}
	cand, err := loadCSV(candidateCSV)
	if err != nil {
		return reportPayload{}, err
	}

	refProfiles := profileColumns(ref)
	candProfiles := profileColumns(cand)
	keyMatch := findKeyMatch(ref, cand, refProfiles, candProfiles)
	if !keyMatch.FoundUsableMatch {
		return zeroResult(ref, cand, refProfiles, candProfiles, keyMatch, rowAlignmentPayload{}), nil
	}

	refKey := derefStr(keyMatch.ReferenceColumn)
	candKey := derefStr(keyMatch.CandidateColumn)
	alignment := alignRowsByKey(ref, cand, refKey, candKey)
	if alignment.MatchedRows == 0 {
		return zeroResult(ref, cand, refProfiles, candProfiles, keyMatch, alignment), nil
	}

	columnMapping := mapColumns(ref, cand, refProfiles, candProfiles, alignment.Pairs, sampleSizeMapping)
	scores := scoreColumns(ref, cand, alignment.Pairs, columnMapping.Mapping)
	scores.OverallScoreWithCoverage = scores.DatasetSimilarityEqualWeighted * alignment.CoverageReference

	return reportPayload{
		Status: ternary(alignment.Complete, "ok", "partial_key_match"),
		Config: configPayload{
			ReferenceCSV:             ref.Path,
			CandidateCSV:             cand.Path,
			SampleSizeMapping:        sampleSizeMapping,
			ColumnWeighting:          map[string]string{"columns": "equal"},
			MissingReferenceColScore: 0.0,
			ExtraCandidatePenalize:   false,
		},
		ReferenceProfile: refProfilePayload{
			RowCount:      len(ref.Rows),
			ColumnCount:   len(ref.Headers),
			UniqueColumns: uniqueColumns(refProfiles, ref.Headers),
		},
		CandidateProfile: candProfilePayload{
			RowCount:    len(cand.Rows),
			ColumnCount: len(cand.Headers),
		},
		RowAlignment:  alignment.withoutPairs(),
		KeyMatch:      keyMatch,
		ColumnMapping: columnMapping,
		Scores:        scores,
		Summary:       buildSummary(ternary(alignment.Complete, "ok", "partial_key_match"), alignment, keyMatch, scores),
	}, nil
}

func loadCSV(path string) (csvTable, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return csvTable{}, err
	}
	b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF})
	r := csv.NewReader(bytes.NewReader(b))
	r.FieldsPerRecord = -1
	headers, err := r.Read()
	if err != nil {
		return csvTable{}, err
	}
	var rows []map[string]string
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return csvTable{}, err
		}
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(rec) {
				row[h] = rec[i]
			} else {
				row[h] = ""
			}
		}
		rows = append(rows, row)
	}
	return csvTable{Path: path, Headers: headers, Rows: rows}, nil
}

func zeroResult(ref, cand csvTable, refProfiles, candProfiles map[string]colProfile, keyMatch keyMatchPayload, alignment rowAlignmentPayload) reportPayload {
	if alignment.ReferenceRows == 0 && alignment.CandidateRows == 0 {
		alignment = rowAlignmentPayload{
			Complete:          false,
			MatchedRows:       0,
			ReferenceRows:     len(ref.Rows),
			CandidateRows:     len(cand.Rows),
			CoverageReference: 0,
			CoverageCandidate: 0,
		}
	}
	per := make([]perColumnScore, 0, len(ref.Headers))
	for _, h := range ref.Headers {
		per = append(per, perColumnScore{
			ReferenceColumn: h,
			CandidateColumn: nil,
			Similarity:      0,
			Matched:         false,
			Reason:          "no_complete_key_match",
		})
	}
	return reportPayload{
		Status: "no_complete_key_match",
		Summary: buildSummary("no_complete_key_match", alignment, keyMatch, scoresPayload{
			DatasetSimilarityEqualWeighted: 0,
			OverallScoreWithCoverage:       0,
			MappedReferenceColumns:         0,
			ReferenceColumnsTotal:          len(ref.Headers),
		}),
		Config: configPayload{
			ReferenceCSV:             ref.Path,
			CandidateCSV:             cand.Path,
			ColumnWeighting:          map[string]string{"columns": "equal"},
			MissingReferenceColScore: 0.0,
			ExtraCandidatePenalize:   false,
		},
		ReferenceProfile: refProfilePayload{
			RowCount:      len(ref.Rows),
			ColumnCount:   len(ref.Headers),
			UniqueColumns: uniqueColumns(refProfiles, ref.Headers),
		},
		CandidateProfile: candProfilePayload{RowCount: len(cand.Rows), ColumnCount: len(cand.Headers)},
		RowAlignment:     alignment.withoutPairs(),
		KeyMatch:         keyMatch,
		ColumnMapping: columnMappingPayload{
			Mapping:              map[string]mappingPair{},
			CandidateUnmatched:   append([]string(nil), cand.Headers...),
			ReferenceUnmatched:   append([]string(nil), ref.Headers...),
			MappingConfidenceAvg: 0,
			PairCandidatesTop:    []mappingPair{},
		},
		Scores: scoresPayload{
			DatasetSimilarityEqualWeighted: 0,
			OverallScoreWithCoverage:       0,
			MappedReferenceColumns:         0,
			ReferenceColumnsTotal:          len(ref.Headers),
			PerReferenceColumn:             per,
		},
	}
}

func buildSummary(status string, alignment rowAlignmentPayload, keyMatch keyMatchPayload, scores scoresPayload) summaryPayload {
	return summaryPayload{
		Status:                         status,
		DatasetSimilarityEqualWeighted: scores.DatasetSimilarityEqualWeighted,
		CoverageReference:              alignment.CoverageReference,
		CoverageCandidate:              alignment.CoverageCandidate,
		OverallScoreWithCoverage:       scores.OverallScoreWithCoverage,
		MatchedRows:                    alignment.MatchedRows,
		ReferenceRows:                  alignment.ReferenceRows,
		CandidateRows:                  alignment.CandidateRows,
		MappedReferenceColumns:         scores.MappedReferenceColumns,
		ReferenceColumnsTotal:          scores.ReferenceColumnsTotal,
		KeyMatchMode:                   keyMatch.MatchMode,
		KeyReferenceColumn:             keyMatch.ReferenceColumn,
		KeyCandidateColumn:             keyMatch.CandidateColumn,
	}
}

func (r rowAlignmentPayload) withoutPairs() rowAlignmentPayload {
	r.Pairs = nil
	return r
}

func profileColumns(table csvTable) map[string]colProfile {
	out := make(map[string]colProfile, len(table.Headers))
	rowCount := len(table.Rows)
	for _, h := range table.Headers {
		vals := make([]string, 0, rowCount)
		for _, row := range table.Rows {
			vals = append(vals, row[h])
		}
		nonEmpty := make([]string, 0, len(vals))
		canonSet := make(map[string]struct{})
		for _, v := range vals {
			if isEmpty(v) {
				continue
			}
			nonEmpty = append(nonEmpty, v)
			canonSet[canonicalScalar(v)] = struct{}{}
		}
		nonEmptyCount := len(nonEmpty)
		uniqNonEmptyCount := len(canonSet)
		isUnique := nonEmptyCount > 0 && uniqNonEmptyCount == nonEmptyCount

		sampleN := min(500, nonEmptyCount)
		numericHits, boolHits := 0, 0
		var totalLen float64
		maxLen := 0
		for i := 0; i < sampleN; i++ {
			v := nonEmpty[i]
			if _, ok := parseDecimal(v); ok {
				numericHits++
			}
			if _, ok := parseBool(v); ok {
				boolHits++
			}
			l := len(normalizeText(v))
			totalLen += float64(l)
			if l > maxLen {
				maxLen = l
			}
		}
		var numRatio, boolRatio, avgLen float64
		if sampleN > 0 {
			numRatio = float64(numericHits) / float64(sampleN)
			boolRatio = float64(boolHits) / float64(sampleN)
			avgLen = totalLen / float64(sampleN)
		}
		uniqRatio := 0.0
		if nonEmptyCount > 0 {
			uniqRatio = float64(uniqNonEmptyCount) / float64(nonEmptyCount)
		}

		out[h] = colProfile{
			RowCount:                rowCount,
			NonEmptyCount:           nonEmptyCount,
			NullCount:               rowCount - nonEmptyCount,
			UniqueNonEmptyCount:     uniqNonEmptyCount,
			IsUniqueNonEmpty:        isUnique,
			UniquenessRatioNonEmpty: uniqRatio,
			NumericRatio:            numRatio,
			BoolRatio:               boolRatio,
			AvgLenSample:            avgLen,
			MaxLenSample:            float64(maxLen),
			HeaderTokens:            headerTokens(h),
		}
	}
	return out
}

func findKeyMatch(ref, cand csvTable, refProfiles, candProfiles map[string]colProfile) keyMatchPayload {
	_ = candProfiles
	candidates := make([]keyCandidate, 0)
	for _, refCol := range ref.Headers {
		if !refProfiles[refCol].IsUniqueNonEmpty {
			continue
		}
		refVals, refSet := nonEmptyCanonValues(ref.Rows, refCol)
		if len(refSet) != len(refVals) {
			continue
		}
		for _, candCol := range cand.Headers {
			candVals, candSet := nonEmptyCanonValues(cand.Rows, candCol)
			if len(candSet) != len(candVals) {
				continue
			}
			intersection := setIntersectionCount(refSet, candSet)
			if intersection == 0 {
				continue
			}
			complete := len(ref.Rows) == len(cand.Rows) && len(candVals) == len(refVals) && setsEqual(refSet, candSet)
			candCoverage := float64(intersection) / maxFloat(float64(len(candSet)), 1)
			refCoverage := float64(intersection) / maxFloat(float64(len(refSet)), 1)
			hScore := headerSimilarity(refCol, candCol)
			keyScore := ternaryFloat(complete, 10.0, 0.0) + (candCoverage * 2.0) + refCoverage + hScore
			candidates = append(candidates, keyCandidate{
				ReferenceColumn:      refCol,
				CandidateColumn:      candCol,
				CompleteSetMatch:     complete,
				IntersectionCount:    intersection,
				CandidateKeyCoverage: round6(candCoverage),
				ReferenceKeyCoverage: round6(refCoverage),
				HeaderSimilarity:     round6(hScore),
				ReferenceNonEmpty:    len(refVals),
				CandidateNonEmpty:    len(candVals),
				Score:                keyScore,
			})
		}
	}
	if len(candidates) == 0 {
		return keyMatchPayload{
			FoundUsableMatch:   false,
			FoundCompleteMatch: false,
			ReferenceColumn:    nil,
			CandidateColumn:    nil,
			Reason:             "no_exact_or_partial_unique_key_match",
			Candidates:         []keyCandidate{},
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].ReferenceNonEmpty > candidates[j].ReferenceNonEmpty
		}
		return candidates[i].Score > candidates[j].Score
	})
	best := candidates[0]
	refCol := best.ReferenceColumn
	candCol := best.CandidateColumn
	mode := "partial"
	reason := "partial_unique_key_overlap_match"
	if best.CompleteSetMatch {
		mode = "complete"
		reason = "exact_unique_key_set_match"
	}
	topN := min(10, len(candidates))
	return keyMatchPayload{
		FoundUsableMatch:   best.IntersectionCount > 0,
		FoundCompleteMatch: best.CompleteSetMatch,
		MatchMode:          mode,
		ReferenceColumn:    &refCol,
		CandidateColumn:    &candCol,
		Reason:             reason,
		Candidates:         candidates[:topN],
	}
}

func alignRowsByKey(ref, cand csvTable, refKey, candKey string) rowAlignmentPayload {
	refIndex := make(map[string]int, len(ref.Rows))
	dupRef := 0
	for i, row := range ref.Rows {
		k := canonicalScalar(row[refKey])
		if k == "" {
			continue
		}
		if _, exists := refIndex[k]; exists {
			dupRef++
			continue
		}
		refIndex[k] = i
	}
	pairs := make([][2]int, 0, len(cand.Rows))
	seenRef := make(map[int]struct{}, len(cand.Rows))
	missing := 0
	dupCandMatches := 0
	for ci, row := range cand.Rows {
		k := canonicalScalar(row[candKey])
		if k == "" {
			missing++
			continue
		}
		ri, ok := refIndex[k]
		if !ok {
			missing++
			continue
		}
		if _, exists := seenRef[ri]; exists {
			dupCandMatches++
			continue
		}
		seenRef[ri] = struct{}{}
		pairs = append(pairs, [2]int{ri, ci})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	matched := len(pairs)
	complete := dupRef == 0 && dupCandMatches == 0 && missing == 0 && matched == len(ref.Rows) && matched == len(cand.Rows)
	return rowAlignmentPayload{
		Complete:                      complete,
		ReferenceKey:                  refKey,
		CandidateKey:                  candKey,
		MatchedRows:                   matched,
		ReferenceRows:                 len(ref.Rows),
		CandidateRows:                 len(cand.Rows),
		CoverageReference:             safeDiv(float64(matched), float64(len(ref.Rows))),
		CoverageCandidate:             safeDiv(float64(matched), float64(len(cand.Rows))),
		DuplicateReferenceKeys:        dupRef,
		DuplicateCandidateMatches:     dupCandMatches,
		MissingCandidateKeysOrMissing: missing,
		Pairs:                         pairs,
	}
}

func mapColumns(ref, cand csvTable, refProfiles, candProfiles map[string]colProfile, pairs [][2]int, sampleSize int) columnMappingPayload {
	samplePairs := pairs
	if sampleSize > 0 && len(samplePairs) > sampleSize {
		samplePairs = samplePairs[:sampleSize]
	}
	allPairs := make([]mappingPair, 0, len(ref.Headers)*len(cand.Headers))
	for _, refCol := range ref.Headers {
		for _, candCol := range cand.Headers {
			h := headerSimilarity(refCol, candCol)
			t := typeCompatibilityScore(refProfiles[refCol], candProfiles[candCol])
			s := sampleColumnSimilarityFast(ref, cand, samplePairs, refCol, candCol)
			conf := (0.35 * h) + (0.10 * t) + (0.55 * s)
			allPairs = append(allPairs, mappingPair{
				ReferenceColumn:   refCol,
				CandidateColumn:   candCol,
				HeaderSimilarity:  round6(h),
				TypeCompatibility: round6(t),
				SampleSimilarity:  round6(s),
				MappingConfidence: round6(conf),
			})
		}
	}
	sort.Slice(allPairs, func(i, j int) bool {
		a, b := allPairs[i], allPairs[j]
		if a.MappingConfidence == b.MappingConfidence {
			if a.SampleSimilarity == b.SampleSimilarity {
				return a.HeaderSimilarity > b.HeaderSimilarity
			}
			return a.SampleSimilarity > b.SampleSimilarity
		}
		return a.MappingConfidence > b.MappingConfidence
	})

	usedRef := map[string]struct{}{}
	usedCand := map[string]struct{}{}
	mapping := map[string]mappingPair{}
	var confs []float64
	for _, p := range allPairs {
		if _, ok := usedRef[p.ReferenceColumn]; ok {
			continue
		}
		if _, ok := usedCand[p.CandidateColumn]; ok {
			continue
		}
		if p.MappingConfidence < 0.55 && p.SampleSimilarity < 0.85 {
			continue
		}
		mapping[p.ReferenceColumn] = p
		usedRef[p.ReferenceColumn] = struct{}{}
		usedCand[p.CandidateColumn] = struct{}{}
		confs = append(confs, p.MappingConfidence)
	}
	refUnmatched := make([]string, 0)
	for _, h := range ref.Headers {
		if _, ok := usedRef[h]; !ok {
			refUnmatched = append(refUnmatched, h)
		}
	}
	candUnmatched := make([]string, 0)
	for _, h := range cand.Headers {
		if _, ok := usedCand[h]; !ok {
			candUnmatched = append(candUnmatched, h)
		}
	}
	topN := min(50, len(allPairs))
	return columnMappingPayload{
		Mapping:              mapping,
		ReferenceUnmatched:   refUnmatched,
		CandidateUnmatched:   candUnmatched,
		MappingConfidenceAvg: avgFloat(confs),
		PairCandidatesTop:    allPairs[:topN],
	}
}

func scoreColumns(ref, cand csvTable, pairs [][2]int, mapping map[string]mappingPair) scoresPayload {
	per := make([]perColumnScore, 0, len(ref.Headers))
	total := 0.0
	mapped := 0
	for _, refCol := range ref.Headers {
		mp, ok := mapping[refCol]
		if !ok {
			per = append(per, perColumnScore{
				ReferenceColumn: refCol,
				CandidateColumn: nil,
				Similarity:      0,
				Matched:         false,
			})
			continue
		}
		s := fullColumnSimilarity(ref, cand, pairs, refCol, mp.CandidateColumn)
		total += s
		mapped++
		candCol := mp.CandidateColumn
		per = append(per, perColumnScore{
			ReferenceColumn:   refCol,
			CandidateColumn:   &candCol,
			Similarity:        s,
			Matched:           true,
			MappingConfidence: mp.MappingConfidence,
			RowCountScored:    len(pairs),
			HeaderSimilarity:  mp.HeaderSimilarity,
			SampleSimilarity:  mp.SampleSimilarity,
		})
	}
	ds := safeDiv(total, float64(len(ref.Headers)))
	return scoresPayload{
		DatasetSimilarityEqualWeighted: ds,
		MappedReferenceColumns:         mapped,
		ReferenceColumnsTotal:          len(ref.Headers),
		PerReferenceColumn:             per,
	}
}

func sampleColumnSimilarityFast(ref, cand csvTable, pairs [][2]int, refCol, candCol string) float64 {
	if len(pairs) == 0 {
		return 0
	}
	exact := 0.0
	samePresence := 0.0
	for _, p := range pairs {
		rv := ref.Rows[p[0]][refCol]
		cv := cand.Rows[p[1]][candCol]
		re := isEmpty(rv)
		ce := isEmpty(cv)
		if re == ce {
			samePresence += 1
		}
		if canonicalScalar(rv) == canonicalScalar(cv) {
			exact += 1
		}
	}
	n := float64(len(pairs))
	return (0.85 * (exact / n)) + (0.15 * (samePresence / n))
}

func fullColumnSimilarity(ref, cand csvTable, pairs [][2]int, refCol, candCol string) float64 {
	if len(pairs) == 0 {
		return 0
	}
	sum := 0.0
	for _, p := range pairs {
		sum += valueSimilarity(ref.Rows[p[0]][refCol], cand.Rows[p[1]][candCol])
	}
	return sum / float64(len(pairs))
}

func valueSimilarity(a, b string) float64 {
	if isEmpty(a) && isEmpty(b) {
		return 1
	}
	if isEmpty(a) || isEmpty(b) {
		return 0
	}
	an := normalizeText(a)
	bn := normalizeText(b)
	if an == bn {
		return 1
	}
	if ab, ok := parseBool(an); ok {
		if bb, ok2 := parseBool(bn); ok2 {
			if ab == bb {
				return 1
			}
			return 0
		}
	}
	if ad, ok := parseDecimal(an); ok {
		if bd, ok2 := parseDecimal(bn); ok2 {
			if ad.Cmp(bd) == 0 {
				return 1
			}
			af, _ := new(big.Float).SetRat(ad).Float64()
			bf, _ := new(big.Float).SetRat(bd).Float64()
			denom := maxFloat(math.Abs(af), math.Abs(bf))
			denom = maxFloat(denom, 1)
			return math.Max(0, 1-(math.Abs(af-bf)/denom))
		}
	}
	return normalizedLevenshteinSimilarity(an, bn)
}

func normalizedLevenshteinSimilarity(a, b string) float64 {
	if a == b {
		return 1
	}
	if a == "" && b == "" {
		return 1
	}
	if a == "" || b == "" {
		return 0
	}
	dist := levenshteinDistance(a, b)
	denom := max(len([]rune(a)), len([]rune(b)))
	if denom == 0 {
		return 1
	}
	return math.Max(0, 1-(float64(dist)/float64(denom)))
}

func levenshteinDistance(a, b string) int {
	ar := []rune(a)
	br := []rune(b)
	if string(ar) == string(br) {
		return 0
	}
	if len(ar) < len(br) {
		ar, br = br, ar
	}
	if len(br) == 0 {
		return len(ar)
	}
	prev := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}
	for i, ca := range ar {
		curr := make([]int, len(br)+1)
		curr[0] = i + 1
		for j, cb := range br {
			ins := curr[j] + 1
			del := prev[j+1] + 1
			sub := prev[j]
			if ca != cb {
				sub++
			}
			curr[j+1] = min3(ins, del, sub)
		}
		prev = curr
	}
	return prev[len(prev)-1]
}

func headerSimilarity(a, b string) float64 {
	at := headerTokens(a)
	bt := headerTokens(b)
	aNorm := strings.Join(at, "")
	bNorm := strings.Join(bt, "")
	if aNorm == "" && bNorm == "" {
		return 1
	}
	seq := normalizedLevenshteinSimilarity(aNorm, bNorm)
	aSet := make(map[string]struct{}, len(at))
	bSet := make(map[string]struct{}, len(bt))
	for _, t := range at {
		aSet[t] = struct{}{}
	}
	for _, t := range bt {
		bSet[t] = struct{}{}
	}
	var jacc float64
	if len(aSet) == 0 && len(bSet) == 0 {
		jacc = 1
	} else if len(aSet) == 0 || len(bSet) == 0 {
		jacc = 0
	} else {
		jacc = float64(setIntersectionCount(aSet, bSet)) / float64(setUnionCount(aSet, bSet))
	}
	if seq > jacc {
		return seq
	}
	return jacc
}

func typeCompatibilityScore(refP, candP colProfile) float64 {
	rNum, cNum := refP.NumericRatio, candP.NumericRatio
	rBool, cBool := refP.BoolRatio, candP.BoolRatio
	if rBool >= 0.9 && cBool >= 0.9 {
		return 1
	}
	if (rBool >= 0.9) != (cBool >= 0.9) {
		return 0.1
	}
	if rNum >= 0.9 && cNum >= 0.9 {
		return 1
	}
	if (rNum >= 0.9) != (cNum >= 0.9) {
		return 0.2
	}
	return 0.8
}

func isEmpty(v string) bool { return strings.TrimSpace(v) == "" }

func normalizeText(v string) string { return strings.TrimSpace(v) }

func parseBool(v string) (bool, bool) {
	s := strings.ToLower(normalizeText(v))
	switch s {
	case "true", "1", "yes", "y":
		return true, true
	case "false", "0", "no", "n":
		return false, true
	default:
		return false, false
	}
}

func parseDecimal(v string) (*big.Rat, bool) {
	s := normalizeText(v)
	if s == "" || !reNumeric.MatchString(s) {
		return nil, false
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return nil, false
	}
	return r, true
}

func canonicalScalar(v string) string {
	if isEmpty(v) {
		return ""
	}
	if b, ok := parseBool(v); ok {
		if b {
			return "true"
		}
		return "false"
	}
	if r, ok := parseDecimal(v); ok {
		_ = r
		return canonicalDecimalString(v)
	}
	return normalizeText(v)
}

func canonicalDecimalString(v string) string {
	s := normalizeText(v)
	if s == "" {
		return ""
	}
	sign := ""
	if strings.HasPrefix(s, "+") {
		s = s[1:]
	} else if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	intPart, fracPart, hasDot := s, "", false
	if i := strings.IndexByte(s, '.'); i >= 0 {
		hasDot = true
		intPart = s[:i]
		fracPart = s[i+1:]
	}
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}
	if hasDot {
		fracPart = strings.TrimRight(fracPart, "0")
	}
	if fracPart == "" {
		if intPart == "0" {
			return "0"
		}
		return sign + intPart
	}
	if intPart == "0" && fracPart == "" {
		return "0"
	}
	return sign + intPart + "." + fracPart
}

func headerTokens(name string) []string {
	raw := reToken.FindAllString(strings.ToLower(name), -1)
	tokens := make([]string, 0, len(raw))
	for _, t := range raw {
		ct := canonHeaderToken(t)
		if ct != "" {
			tokens = append(tokens, ct)
		}
	}
	return tokens
}

func canonHeaderToken(t string) string {
	if v, ok := headerTokenAliases[t]; ok {
		return v
	}
	return t
}

func nonEmptyCanonValues(rows []map[string]string, col string) ([]string, map[string]struct{}) {
	vals := make([]string, 0, len(rows))
	set := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		v := r[col]
		if isEmpty(v) {
			continue
		}
		c := canonicalScalar(v)
		vals = append(vals, c)
		set[c] = struct{}{}
	}
	return vals, set
}

func setsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func setIntersectionCount(a, b map[string]struct{}) int {
	if len(a) > len(b) {
		a, b = b, a
	}
	n := 0
	for k := range a {
		if _, ok := b[k]; ok {
			n++
		}
	}
	return n
}

func setUnionCount(a, b map[string]struct{}) int {
	n := len(a)
	for k := range b {
		if _, ok := a[k]; !ok {
			n++
		}
	}
	return n
}

func uniqueColumns(profiles map[string]colProfile, orderedHeaders []string) []string {
	out := make([]string, 0)
	for _, h := range orderedHeaders {
		if profiles[h].IsUniqueNonEmpty {
			out = append(out, h)
		}
	}
	return out
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

func avgFloat(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min3(a, b, c int) int {
	if a > b {
		a = b
	}
	if a > c {
		return c
	}
	return a
}

func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func ternaryFloat(cond bool, a, b float64) float64 {
	if cond {
		return a
	}
	return b
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
