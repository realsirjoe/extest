package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testdataPath(name string) string {
	return filepath.Join("..", "..", "testdata", name)
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= 1e-12
}

func TestCompareCSV_Candidate1FullMatch(t *testing.T) {
	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		testdataPath("sample_products_candidate1_500.csv"),
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected dataset similarity 1.0, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 1.0) || !almostEqual(report.RowAlignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected full coverage 1/1, got %.15f / %.15f", report.RowAlignment.CoverageReference, report.RowAlignment.CoverageCandidate)
	}
	if !almostEqual(report.Scores.OverallScoreWithCoverage, 1.0) {
		t.Fatalf("expected overall score 1.0, got %.15f", report.Scores.OverallScoreWithCoverage)
	}
}

func TestCompareCSV_Candidate2SingleMutationSlightlyBelowOne(t *testing.T) {
	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		testdataPath("sample_products_candidate2_500.csv"),
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	score := report.Scores.DatasetSimilarityEqualWeighted
	if !(score < 1.0) {
		t.Fatalf("expected score < 1.0, got %.15f", score)
	}
	if !(score > 0.9999) {
		t.Fatalf("expected score close to 1.0, got %.15f", score)
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 1.0) || !almostEqual(report.RowAlignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected full coverage 1/1, got %.15f / %.15f", report.RowAlignment.CoverageReference, report.RowAlignment.CoverageCandidate)
	}
	if !almostEqual(report.Scores.OverallScoreWithCoverage, score) {
		t.Fatalf("expected overall score to equal similarity under full coverage; got overall=%.15f score=%.15f", report.Scores.OverallScoreWithCoverage, score)
	}
}

func TestCompareCSV_Candidate3SubsetHasPerfectSimilarityButLowerCoverage(t *testing.T) {
	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		testdataPath("sample_products_candidate3_100.csv"),
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "partial_key_match" {
		t.Fatalf("expected status partial_key_match, got %q", report.Status)
	}
	if report.KeyMatch.MatchMode != "partial" {
		t.Fatalf("expected key match mode partial, got %q", report.KeyMatch.MatchMode)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected dataset similarity 1.0, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 0.2) {
		t.Fatalf("expected reference coverage 0.2, got %.15f", report.RowAlignment.CoverageReference)
	}
	if !almostEqual(report.RowAlignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected candidate coverage 1.0, got %.15f", report.RowAlignment.CoverageCandidate)
	}
	if !almostEqual(report.Scores.OverallScoreWithCoverage, 0.2) {
		t.Fatalf("expected overall score with coverage 0.2, got %.15f", report.Scores.OverallScoreWithCoverage)
	}
}

func TestCompareCSV_MissingSingleRowDropsCoverageBelowOne(t *testing.T) {
	tmpDir := t.TempDir()
	candidate499 := filepath.Join(tmpDir, "candidate_499.csv")
	baseRows, err := readCSVRows(testdataPath("sample_products_candidate1_500.csv"))
	if err != nil {
		t.Fatalf("readCSVRows error: %v", err)
	}
	rowIdx := firstRowIndexWithNonEmptyColumn(baseRows.Header, baseRows.Records, "gtin_code")
	if rowIdx < 0 {
		t.Fatalf("no candidate row with non-empty gtin_code found")
	}
	if err := writeCSVWithoutRow(
		testdataPath("sample_products_candidate1_500.csv"),
		candidate499,
		rowIdx,
	); err != nil {
		t.Fatalf("writeCSVWithoutRow error: %v", err)
	}

	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		candidate499,
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "partial_key_match" {
		t.Fatalf("expected status partial_key_match, got %q", report.Status)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected similarity 1.0 for remaining matched rows, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 499.0/500.0) {
		t.Fatalf("expected reference coverage 499/500, got %.15f", report.RowAlignment.CoverageReference)
	}
	if !almostEqual(report.RowAlignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected candidate coverage 1.0, got %.15f", report.RowAlignment.CoverageCandidate)
	}
	if !(report.Scores.OverallScoreWithCoverage < 1.0) {
		t.Fatalf("expected overall score with coverage < 1.0, got %.15f", report.Scores.OverallScoreWithCoverage)
	}
	if !almostEqual(report.Scores.OverallScoreWithCoverage, 499.0/500.0) {
		t.Fatalf("expected overall score with coverage 499/500, got %.15f", report.Scores.OverallScoreWithCoverage)
	}
}

func TestCompareCSV_NoUsableKeyMatchWhenCandidateKeysRemoved(t *testing.T) {
	tmpDir := t.TempDir()
	candidateNoKeys := filepath.Join(tmpDir, "candidate_no_keys.csv")
	if err := writeCSVWithoutColumns(
		testdataPath("sample_products_candidate1_500.csv"),
		candidateNoKeys,
		map[string]struct{}{
			"gtin_code": {},
			"dan_code":  {},
		},
		true,
	); err != nil {
		t.Fatalf("writeCSVWithoutColumns error: %v", err)
	}

	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		candidateNoKeys,
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "no_complete_key_match" {
		t.Fatalf("expected status no_complete_key_match, got %q", report.Status)
	}
	if report.KeyMatch.FoundUsableMatch {
		t.Fatalf("expected no usable key match")
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 0.0) {
		t.Fatalf("expected dataset score 0, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 0.0) || !almostEqual(report.RowAlignment.CoverageCandidate, 0.0) {
		t.Fatalf("expected zero coverage, got %.15f / %.15f", report.RowAlignment.CoverageReference, report.RowAlignment.CoverageCandidate)
	}
	if !almostEqual(report.Scores.OverallScoreWithCoverage, 0.0) {
		t.Fatalf("expected overall score 0, got %.15f", report.Scores.OverallScoreWithCoverage)
	}
}

func TestCompareCSV_NegativeSampleSizeMappingIsClamped(t *testing.T) {
	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		testdataPath("sample_products_candidate1_500.csv"),
		-5,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected dataset similarity 1.0, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
	if report.Config.SampleSizeMapping != 0 {
		t.Fatalf("expected clamped sample_size_mapping=0, got %d", report.Config.SampleSizeMapping)
	}
}

func TestCompareCSV_MissingCandidateColumnScoresZeroForReferenceColumn(t *testing.T) {
	tmpDir := t.TempDir()
	candidateMissingCol := filepath.Join(tmpDir, "candidate_missing_col.csv")
	if err := writeCSVWithoutColumns(
		testdataPath("sample_products_candidate1_500.csv"),
		candidateMissingCol,
		map[string]struct{}{"rating_score": {}},
		false,
	); err != nil {
		t.Fatalf("writeCSVWithoutColumns error: %v", err)
	}

	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		candidateMissingCol,
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if report.Scores.MappedReferenceColumns != 40 {
		t.Fatalf("expected 40 mapped reference columns, got %d", report.Scores.MappedReferenceColumns)
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 1.0) || !almostEqual(report.RowAlignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected full coverage 1/1, got %.15f / %.15f", report.RowAlignment.CoverageReference, report.RowAlignment.CoverageCandidate)
	}
	var found bool
	for _, col := range report.Scores.PerReferenceColumn {
		if col.ReferenceColumn == "rating_value" {
			found = true
			if col.Matched {
				t.Fatalf("expected rating_value to be unmatched when rating_score column is removed")
			}
			if !almostEqual(col.Similarity, 0.0) {
				t.Fatalf("expected rating_value similarity 0, got %.15f", col.Similarity)
			}
		}
	}
	if !found {
		t.Fatalf("expected per-column score for rating_value")
	}
	if !(report.Scores.DatasetSimilarityEqualWeighted < 1.0) {
		t.Fatalf("expected overall similarity < 1, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
}

func TestCompareCSV_ExtraCandidateColumnDoesNotPenalize(t *testing.T) {
	tmpDir := t.TempDir()
	candidateExtra := filepath.Join(tmpDir, "candidate_extra_col.csv")
	if err := writeCSVWithExtraColumn(
		testdataPath("sample_products_candidate1_500.csv"),
		candidateExtra,
		"extra_noise",
		func(i int, rec []string, header []string) string {
			return fmt.Sprintf("noise_%03d", i)
		},
	); err != nil {
		t.Fatalf("writeCSVWithExtraColumn error: %v", err)
	}

	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		candidateExtra,
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected similarity 1.0, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
	if !containsString(report.ColumnMapping.CandidateUnmatched, "extra_noise") {
		t.Fatalf("expected extra_noise to appear in candidate_unmatched")
	}
}

func TestCompareCSV_NumericFormattingNormalizationKeepsSimilarityOne(t *testing.T) {
	tmpDir := t.TempDir()
	candidateNumericFmt := filepath.Join(tmpDir, "candidate_numeric_fmt.csv")
	if err := writeCSVMutatingRows(
		testdataPath("sample_products_candidate1_500.csv"),
		candidateNumericFmt,
		func(header []string, row []string, rowIdx int) {
			for _, col := range []string{"price_eur_amt", "rating_score"} {
				idx := mustColumnIndex(header, col)
				v := strings.TrimSpace(row[idx])
				if v == "" {
					continue
				}
				if strings.Contains(v, ".") {
					row[idx] = v + "00"
				} else {
					row[idx] = v + ".0"
				}
			}
		},
	); err != nil {
		t.Fatalf("writeCSVMutatingRows error: %v", err)
	}

	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		candidateNumericFmt,
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected similarity 1.0 under numeric normalization, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
}

func TestCompareCSV_BothSideEmptyCellKeepsSimilarity(t *testing.T) {
	tmpDir := t.TempDir()
	refOut := filepath.Join(tmpDir, "ref_both_empty.csv")
	candOut := filepath.Join(tmpDir, "cand_both_empty.csv")

	refRows, err := readCSVRows(testdataPath("sample_products_reference_500.csv"))
	if err != nil {
		t.Fatalf("readCSVRows reference error: %v", err)
	}
	candRows, err := readCSVRows(testdataPath("sample_products_candidate1_500.csv"))
	if err != nil {
		t.Fatalf("readCSVRows candidate error: %v", err)
	}
	refGTINIdx := mustColumnIndex(refRows.Header, "gtin")
	targetGTIN := firstNonEmptyValueInColumn(refRows.Records, refGTINIdx)
	if targetGTIN == "" {
		t.Fatalf("no gtin found in reference testdata")
	}
	if !setCellByKey(refRows.Header, refRows.Records, "gtin", targetGTIN, "brand", "") {
		t.Fatalf("failed to blank reference brand for gtin %s", targetGTIN)
	}
	if !setCellByKey(candRows.Header, candRows.Records, "gtin_code", targetGTIN, "brand_name", "") {
		t.Fatalf("failed to blank candidate brand_name for gtin %s", targetGTIN)
	}
	if err := writeCSVRows(refOut, refRows); err != nil {
		t.Fatalf("writeCSVRows reference error: %v", err)
	}
	if err := writeCSVRows(candOut, candRows); err != nil {
		t.Fatalf("writeCSVRows candidate error: %v", err)
	}

	report, err := compareCSVFiles(refOut, candOut, 256)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if !almostEqual(report.Scores.DatasetSimilarityEqualWeighted, 1.0) {
		t.Fatalf("expected similarity 1.0 when same aligned cell is blank on both sides, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
}

func TestCompareCSV_DuplicateCandidateKeyCausesPartialAlignment(t *testing.T) {
	tmpDir := t.TempDir()
	candidateDup := filepath.Join(tmpDir, "candidate_duplicate_key.csv")
	const candidateKey = "gtin_code"
	baseRows, err := readCSVRows(testdataPath("sample_products_candidate1_500.csv"))
	if err != nil {
		t.Fatalf("readCSVRows error: %v", err)
	}
	rowIdx := firstRowIndexWithNonEmptyColumn(baseRows.Header, baseRows.Records, candidateKey)
	if rowIdx < 0 {
		t.Fatalf("no candidate row with non-empty %s found", candidateKey)
	}
	if err := writeCSVWithDuplicateRow(testdataPath("sample_products_candidate1_500.csv"), candidateDup, rowIdx); err != nil {
		t.Fatalf("writeCSVWithDuplicateRow error: %v", err)
	}

	ref, err := loadCSV(testdataPath("sample_products_reference_500.csv"))
	if err != nil {
		t.Fatalf("loadCSV reference error: %v", err)
	}
	cand, err := loadCSV(candidateDup)
	if err != nil {
		t.Fatalf("loadCSV candidate error: %v", err)
	}
	// This test intentionally targets row-alignment duplicate handling directly.
	// End-to-end key selection under duplicates is heuristic and covered separately.
	alignment := alignRowsByKey(ref, cand, "gtin", candidateKey)
	if alignment.Complete {
		t.Fatalf("expected incomplete alignment with duplicated candidate key row")
	}
	if alignment.DuplicateCandidateMatches < 1 {
		t.Fatalf("expected duplicate candidate matches > 0, got %d", alignment.DuplicateCandidateMatches)
	}
	if !almostEqual(alignment.CoverageReference, 1.0) {
		t.Fatalf("expected reference coverage 1.0, got %.15f", alignment.CoverageReference)
	}
	if !(alignment.CoverageCandidate < 1.0) {
		t.Fatalf("expected candidate coverage < 1.0, got %.15f", alignment.CoverageCandidate)
	}
}

func TestCompareCSV_DuplicateReferenceKeyCausesPartialAlignment(t *testing.T) {
	tmpDir := t.TempDir()
	referenceDup := filepath.Join(tmpDir, "reference_duplicate_key.csv")
	const referenceKey = "gtin"

	baseRows, err := readCSVRows(testdataPath("sample_products_reference_500.csv"))
	if err != nil {
		t.Fatalf("readCSVRows error: %v", err)
	}
	rowIdx := firstRowIndexWithNonEmptyColumn(baseRows.Header, baseRows.Records, referenceKey)
	if rowIdx < 0 {
		t.Fatalf("no reference row with non-empty %s found", referenceKey)
	}
	if err := writeCSVWithDuplicateRow(testdataPath("sample_products_reference_500.csv"), referenceDup, rowIdx); err != nil {
		t.Fatalf("writeCSVWithDuplicateRow error: %v", err)
	}

	ref, err := loadCSV(referenceDup)
	if err != nil {
		t.Fatalf("loadCSV reference error: %v", err)
	}
	cand, err := loadCSV(testdataPath("sample_products_candidate1_500.csv"))
	if err != nil {
		t.Fatalf("loadCSV candidate error: %v", err)
	}
	// This test intentionally targets row-alignment duplicate handling directly.
	// End-to-end key selection under duplicates is heuristic and covered separately.
	alignment := alignRowsByKey(ref, cand, referenceKey, "gtin_code")
	if alignment.Complete {
		t.Fatalf("expected incomplete alignment with duplicated reference key row")
	}
	if alignment.DuplicateReferenceKeys < 1 {
		t.Fatalf("expected duplicate reference keys > 0, got %d", alignment.DuplicateReferenceKeys)
	}
	if !(alignment.CoverageReference < 1.0) {
		t.Fatalf("expected reference coverage < 1.0, got %.15f", alignment.CoverageReference)
	}
	if !almostEqual(alignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected candidate coverage 1.0, got %.15f", alignment.CoverageCandidate)
	}
}

func TestCompareCSV_AlternateKeyChosenWhenGTINColumnMissing(t *testing.T) {
	tmpDir := t.TempDir()
	candidateNoGTIN := filepath.Join(tmpDir, "candidate_no_gtin.csv")
	if err := writeCSVWithoutColumns(
		testdataPath("sample_products_candidate1_500.csv"),
		candidateNoGTIN,
		map[string]struct{}{"gtin_code": {}},
		false,
	); err != nil {
		t.Fatalf("writeCSVWithoutColumns error: %v", err)
	}

	report, err := compareCSVFiles(
		testdataPath("sample_products_reference_500.csv"),
		candidateNoGTIN,
		256,
	)
	if err != nil {
		t.Fatalf("compareCSVFiles error: %v", err)
	}
	if report.Status != "ok" {
		t.Fatalf("expected status ok, got %q", report.Status)
	}
	if !report.KeyMatch.FoundUsableMatch {
		t.Fatalf("expected usable key match")
	}
	if report.KeyMatch.CandidateColumn == nil {
		t.Fatalf("expected candidate key column")
	}
	if *report.KeyMatch.CandidateColumn == "gtin_code" {
		t.Fatalf("expected alternate key, got gtin_code")
	}
	if !almostEqual(report.RowAlignment.CoverageReference, 1.0) || !almostEqual(report.RowAlignment.CoverageCandidate, 1.0) {
		t.Fatalf("expected full coverage 1/1, got %.15f / %.15f", report.RowAlignment.CoverageReference, report.RowAlignment.CoverageCandidate)
	}
	if report.Scores.MappedReferenceColumns != 40 {
		t.Fatalf("expected 40 mapped reference columns when gtin_code is removed, got %d", report.Scores.MappedReferenceColumns)
	}
	if !(report.Scores.DatasetSimilarityEqualWeighted < 1.0) {
		t.Fatalf("expected similarity < 1.0 due to missing gtin column, got %.15f", report.Scores.DatasetSimilarityEqualWeighted)
	}
}

type csvRows struct {
	Header  []string
	Records [][]string
}

func readCSVRows(path string) (csvRows, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return csvRows{}, err
	}
	b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF})
	r := csv.NewReader(bytes.NewReader(b))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return csvRows{}, err
	}
	out := csvRows{Header: append([]string(nil), header...)}
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return csvRows{}, err
		}
		out.Records = append(out.Records, append([]string(nil), rec...))
	}
	return out, nil
}

func writeCSVRows(dst string, rows csvRows) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.UseCRLF = true
	if err := w.Write(rows.Header); err != nil {
		return err
	}
	for _, rec := range rows.Records {
		out := append([]string(nil), rec...)
		for len(out) < len(rows.Header) {
			out = append(out, "")
		}
		if err := w.Write(out); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeCSVWithoutColumns(src, dst string, drop map[string]struct{}, blankValues bool) error {
	rows, err := readCSVRows(src)
	if err != nil {
		return err
	}
	keepIdx := make([]int, 0, len(rows.Header))
	keepHeader := make([]string, 0, len(rows.Header))
	for i, h := range rows.Header {
		if _, remove := drop[h]; remove {
			continue
		}
		keepIdx = append(keepIdx, i)
		keepHeader = append(keepHeader, h)
	}
	out := csvRows{Header: keepHeader, Records: make([][]string, 0, len(rows.Records))}
	for _, rec := range rows.Records {
		dstRec := make([]string, 0, len(keepIdx))
		for _, idx := range keepIdx {
			if idx < len(rec) {
				if blankValues {
					dstRec = append(dstRec, "")
				} else {
					dstRec = append(dstRec, rec[idx])
				}
			} else {
				dstRec = append(dstRec, "")
			}
		}
		out.Records = append(out.Records, dstRec)
	}
	return writeCSVRows(dst, out)
}

func writeCSVWithExtraColumn(src, dst, colName string, valueFn func(int, []string, []string) string) error {
	rows, err := readCSVRows(src)
	if err != nil {
		return err
	}
	rows.Header = append(rows.Header, colName)
	for i := range rows.Records {
		rows.Records[i] = append(rows.Records[i], valueFn(i, rows.Records[i], rows.Header))
	}
	return writeCSVRows(dst, rows)
}

func writeCSVMutatingRows(src, dst string, mutate func(header []string, row []string, rowIdx int)) error {
	rows, err := readCSVRows(src)
	if err != nil {
		return err
	}
	for i := range rows.Records {
		mutate(rows.Header, rows.Records[i], i)
	}
	return writeCSVRows(dst, rows)
}

func writeCSVWithDuplicateRow(src, dst string, rowIdx int) error {
	rows, err := readCSVRows(src)
	if err != nil {
		return err
	}
	if len(rows.Records) == 0 {
		return fmt.Errorf("no rows to duplicate")
	}
	if rowIdx < 0 || rowIdx >= len(rows.Records) {
		return fmt.Errorf("rowIdx out of range: %d", rowIdx)
	}
	dup := append([]string(nil), rows.Records[rowIdx]...)
	rows.Records = append(rows.Records, dup)
	return writeCSVRows(dst, rows)
}

func writeCSVWithoutRow(src, dst string, rowIdx int) error {
	rows, err := readCSVRows(src)
	if err != nil {
		return err
	}
	if rowIdx < 0 || rowIdx >= len(rows.Records) {
		return fmt.Errorf("rowIdx out of range: %d", rowIdx)
	}
	out := csvRows{
		Header:  append([]string(nil), rows.Header...),
		Records: make([][]string, 0, len(rows.Records)-1),
	}
	for i, rec := range rows.Records {
		if i == rowIdx {
			continue
		}
		out.Records = append(out.Records, append([]string(nil), rec...))
	}
	return writeCSVRows(dst, out)
}

func mustColumnIndex(header []string, col string) int {
	for i, h := range header {
		if h == col {
			return i
		}
	}
	panic("missing column: " + col)
}

func firstNonEmptyValueInColumn(records [][]string, idx int) string {
	for _, rec := range records {
		if idx < len(rec) && strings.TrimSpace(rec[idx]) != "" {
			return rec[idx]
		}
	}
	return ""
}

func firstRowIndexWithNonEmptyColumn(header []string, records [][]string, col string) int {
	idx := mustColumnIndex(header, col)
	for i, rec := range records {
		if idx < len(rec) && strings.TrimSpace(rec[idx]) != "" {
			return i
		}
	}
	return -1
}

func setCellByKey(header []string, records [][]string, keyCol, keyVal, targetCol, targetVal string) bool {
	keyIdx := mustColumnIndex(header, keyCol)
	targetIdx := mustColumnIndex(header, targetCol)
	for i := range records {
		if keyIdx >= len(records[i]) {
			continue
		}
		if records[i][keyIdx] != keyVal {
			continue
		}
		for len(records[i]) <= targetIdx {
			records[i] = append(records[i], "")
		}
		records[i][targetIdx] = targetVal
		return true
	}
	return false
}

func containsString(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}
