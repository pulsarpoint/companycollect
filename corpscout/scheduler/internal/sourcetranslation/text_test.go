package sourcetranslation

import "testing"

func TestTermKeyNormalizesTrimAndCase(t *testing.T) {
	first := TermKey("  Aktsiaselts  ")
	second := TermKey("aktsiaselts")
	if first != second {
		t.Fatalf("expected normalized keys to match: %s != %s", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("expected sha256 hex key length 64, got %d", len(first))
	}
}

func TestNormalizeTextTrimsLowercasesAndKeepsInternalSpacing(t *testing.T) {
	got := NormalizeText("  OSA  ÜHING  ")
	want := "osa  ühing"
	if got != want {
		t.Fatalf("NormalizeText() = %q, want %q", got, want)
	}
}

func TestTermKeyNormalizesUnicodeComposition(t *testing.T) {
	composed := TermKey("ÜHING")
	decomposed := TermKey("U\u0308HING")
	if composed != decomposed {
		t.Fatalf("expected unicode-normalized keys to match: %s != %s", composed, decomposed)
	}
}
