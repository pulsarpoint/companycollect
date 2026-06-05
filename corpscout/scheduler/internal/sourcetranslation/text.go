package sourcetranslation

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"golang.org/x/text/unicode/norm"
)

func NormalizeText(sourceText string) string {
	return norm.NFC.String(strings.ToLower(strings.TrimSpace(sourceText)))
}

func TermKey(sourceText string) string {
	sum := sha256.Sum256([]byte(NormalizeText(sourceText)))
	return hex.EncodeToString(sum[:])
}
