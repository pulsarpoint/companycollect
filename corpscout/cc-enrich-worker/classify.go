package main

import (
	"math"
	"sort"
	"strings"
)

func sqrt32(x float32) float32 { return float32(math.Sqrt(float64(x))) }

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// norm L2-normalizes a vector (used here and by the embed client).
func norm(v []float32) []float32 {
	s := float32(1.0) / (sqrt32(dot(v, v)) + 1e-9)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * s
	}
	return out
}

// division: 2-digit NACE division from a possibly-messy code.
func division(code string) string {
	for i := 0; i+1 < len(code); i++ {
		if code[i] >= '0' && code[i] <= '9' && code[i+1] >= '0' && code[i+1] <= '9' {
			return code[i : i+2]
		}
	}
	if code != "" {
		return strings.ToUpper(code[:1])
	}
	return ""
}

// Classify a normalized page vector against the NACE matrix + page-type prototypes.
func Classify(page []float32, ref *Reference, protos *Prototypes) DomainResult {
	type sc struct {
		i int
		s float32
	}
	sims := make([]sc, len(ref.M))
	for i, row := range ref.M {
		sims[i] = sc{i, dot(page, row)}
	}
	sort.Slice(sims, func(a, b int) bool { return sims[a].s > sims[b].s })

	k := 3
	if len(sims) < k {
		k = len(sims)
	}
	var codes, labels []string
	var scores []float32
	divs := map[string]bool{}
	for n := 0; n < k; n++ {
		idx := sims[n].i
		codes = append(codes, ref.Codes[idx])
		labels = append(labels, ref.Labels[idx])
		scores = append(scores, sims[n].s)
		divs[ref.Divisions[idx]] = true
	}
	margin := scores[0]
	if len(scores) > 1 {
		margin = scores[0] - scores[1]
	}
	consensus := len(divs) == 1

	// page-type prototypes: max similarity + its label
	var ptScore float32
	ptLabel := ""
	for i, row := range protos.P {
		if s := dot(page, row); s > ptScore {
			ptScore, ptLabel = s, protos.Labels[i]
		}
	}
	isPageType := ptScore >= pageTypeThreshold

	r := DomainResult{
		PageTypeScore: ptScore,
		NaceCode:      codes[0], NaceLabel: labels[0], NaceDivision: ref.Divisions[sims[0].i],
		NaceMargin: margin, NaceScore: scores[0],
		NaceMethod: "embedding",
		Top3Codes:  codes, Top3Labels: labels, Top3Scores: scores,
		NaceConfident: (margin >= marginThreshold || consensus) && !isPageType,
	}
	if isPageType {
		r.PageType = ptLabel
	}
	return r
}
