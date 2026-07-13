package tech

import "testing"

func TestBetterVersion(t *testing.T) {
	tests := []struct {
		name               string
		candidate, current string
		want               bool
	}{
		{name: "fills empty", candidate: "6.4", want: true},
		{name: "ignores empty", current: "6.4", want: false},
		{name: "more segments", candidate: "6.3.2", current: "6.4", want: true},
		{name: "fewer segments", candidate: "6.5", current: "6.4.1", want: false},
		{name: "higher numeric version", candidate: "6.5", current: "6.4", want: true},
		{name: "lower numeric version", candidate: "6.3", current: "6.4", want: false},
		{name: "stable textual tie", candidate: "01.0", current: "1.0", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := betterVersion(test.candidate, test.current); got != test.want {
				t.Fatalf("betterVersion(%q, %q) = %v, want %v", test.candidate, test.current, got, test.want)
			}
		})
	}
}
