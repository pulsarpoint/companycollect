package partspec

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input string
		want  []int
	}{
		{input: "0", want: []int{0}},
		{input: "7", want: []int{7}},
		{input: "0-3", want: []int{0, 1, 2, 3}},
	}
	for _, test := range tests {
		got, err := Parse(test.input)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.input, err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("Parse(%q)=%v, want %v", test.input, got, test.want)
		}
	}
	for _, input := range []string{"", "-1", "3-2", "x", "1-2-3", "0-10001"} {
		if _, err := Parse(input); err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", input)
		}
	}
}

func TestFormat(t *testing.T) {
	if got := Format([]int{85, 86, 87, 90, 92, 93}); got != "85-87,90,92-93" {
		t.Fatalf("Format()=%q", got)
	}
	if got := Format(nil); got != "" {
		t.Fatalf("Format(nil)=%q", got)
	}
}
