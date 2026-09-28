package resolve

import (
	"cmp"
	"slices"
	"time"

	"provider_recon/internal/detect/knowledge"
)

// Open ends of a window, as comparable dates.
const (
	beginning = "0001-01-01"
	forever   = "9999-12-31"
)

// ipPiece is one stretch of a record's window with the range that wins it.
// From/To are dates; "" is open, as on Record and IPRange.
type ipPiece struct {
	From, To string
	Range    knowledge.IPRange
}

// ipPieces splits the window [from, to] at every change of the ranges valid
// in it. Each stretch goes to the longest-prefix range valid for all of it
// (ties: lower rule id); stretches no range covers are left out, and adjacent
// stretches won by the same range (e.g. its re-added instance) are merged.
func ipPieces(from, to string, ranges []knowledge.IPRange) []ipPiece {
	f, t := or(from, beginning), or(to, forever)
	ranges = slices.Clone(ranges)
	slices.SortStableFunc(ranges, func(a, b knowledge.IPRange) int {
		return cmp.Or(cmp.Compare(b.Prefix.Bits(), a.Prefix.Bits()), cmp.Compare(a.RuleID, b.RuleID))
	})
	points := []string{f}
	var live []knowledge.IPRange
	for _, r := range ranges {
		rf, rt := or(r.From, beginning), or(r.To, forever)
		if rf > t || rt < f {
			continue
		}
		live = append(live, r)
		if rf > f {
			points = append(points, rf)
		}
		if rt < t {
			points = append(points, nextDay(rt))
		}
	}
	slices.Sort(points)
	points = slices.Compact(points)
	var out []ipPiece
	for i, start := range points {
		end := t
		if i+1 < len(points) {
			end = prevDay(points[i+1])
		}
		for _, r := range live {
			if or(r.From, beginning) > start || or(r.To, forever) < end {
				continue
			}
			if n := len(out); n > 0 && out[n-1].Range.RuleID == r.RuleID && nextDay(out[n-1].To) == start {
				out[n-1].To = end
			} else {
				out = append(out, ipPiece{From: start, To: end, Range: r})
			}
			break
		}
	}
	for i := range out {
		if out[i].From == beginning {
			out[i].From = ""
		}
		if out[i].To == forever {
			out[i].To = ""
		}
	}
	return out
}

func or(date, open string) string {
	if date == "" {
		return open
	}
	return date
}

func nextDay(date string) string { return shift(date, 1) }
func prevDay(date string) string { return shift(date, -1) }

func shift(date string, days int) string {
	d, err := time.Parse(time.DateOnly, date)
	if err != nil {
		return date
	}
	return d.AddDate(0, 0, days).Format(time.DateOnly)
}
