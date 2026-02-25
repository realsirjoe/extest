package main

import (
	"bytes"
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
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

func writeCSVWithoutColumns(src, dst string, drop map[string]struct{}, blankValues bool) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF})
	r := csv.NewReader(bytes.NewReader(b))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return err
	}

	keepIdx := make([]int, 0, len(header))
	keepHeader := make([]string, 0, len(header))
	for i, h := range header {
		if _, remove := drop[h]; remove {
			continue
		}
		keepIdx = append(keepIdx, i)
		keepHeader = append(keepHeader, h)
	}

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
	if err := w.Write(keepHeader); err != nil {
		return err
	}
	for {
		rec, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		out := make([]string, 0, len(keepIdx))
		for _, idx := range keepIdx {
			if idx < len(rec) {
				if blankValues {
					out = append(out, "")
				} else {
					out = append(out, rec[idx])
				}
			} else {
				out = append(out, "")
			}
		}
		if err := w.Write(out); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
