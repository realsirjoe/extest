package main

import (
	"math"
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
		testdataPath("dm_products_reference_500.csv"),
		testdataPath("dm_products_candidate1_500.csv"),
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
		testdataPath("dm_products_reference_500.csv"),
		testdataPath("dm_products_candidate2_500.csv"),
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
		testdataPath("dm_products_reference_500.csv"),
		testdataPath("dm_products_candidate3_100.csv"),
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
