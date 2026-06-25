package main

import (
	"math"
	"sort"
	"strings"

	"cc-enrich-worker/internal/vec"
)

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
		sims[i] = sc{i, vec.Dot(page, row)}
	}
	sort.Slice(sims, func(a, b int) bool { return sims[a].s > sims[b].s })

	allScores := make([]float32, len(sims))
	for i := range sims {
		allScores[i] = sims[i].s
	}

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
		if s := vec.Dot(page, row); s > ptScore {
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
		NaceConfident:  (margin >= marginThreshold || consensus) && !isPageType,
		NaceConfidence: confidenceScore(allScores),
	}
	if isPageType {
		r.PageType = ptLabel
	}
	return r
}

// confidenceScore turns the per-industry cosine scores into a single 0-1 confidence:
// standardize by median + MAD (robust to the few top outliers, removes the per-page
// baseline), then softmax over the top-K standardized scores. High when the top industry
// stands clearly above the bulk; low when scores are mushy or spread across a cluster.
func confidenceScore(sortedDesc []float32) float32 {
	n := len(sortedDesc)
	if n == 0 {
		return 0
	}
	med := median(sortedDesc) // middle element — order doesn't matter
	devs := make([]float32, n)
	for i, s := range sortedDesc {
		devs[i] = absF(s - med)
	}
	mad := median(devs)
	if mad < 1e-6 {
		mad = 1e-6 // degenerate (all scores equal) -> avoid div by zero; softmax -> ~1/K
	}
	kk := confTopK
	if n < kk {
		kk = n
	}
	// sortedDesc[0] is the top, so its standardized z is the max -> p[0] = 1/sum.
	zMax := (sortedDesc[0] - med) / mad
	var sum float64
	for i := 0; i < kk; i++ {
		z := (sortedDesc[i] - med) / mad
		sum += math.Exp(float64((z - zMax) / confTemp))
	}
	return float32(1.0 / sum)
}

func absF(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}

// median of xs (sorts a copy; xs is not mutated).
func median(xs []float32) float32 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	c := append([]float32(nil), xs...)
	sort.Slice(c, func(a, b int) bool { return c[a] < c[b] })
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}
