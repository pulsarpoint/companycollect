package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNonEmptyJSONDefaultsToNamedEmptyObject(t *testing.T) {
	require.Equal(t, []byte("{}"), nonEmptyJSON(nil))
	require.Equal(t, []byte("{}"), nonEmptyJSON([]byte{}))
	require.Equal(t, []byte("{}"), nonEmptyJSON([]byte("null")))
	require.Equal(t, []byte(`{"source":"brreg"}`), nonEmptyJSON([]byte(`{"source":"brreg"}`)))
}

func TestNonEmptyJSONDefaultIsNamed(t *testing.T) {
	source, err := os.ReadFile("company_suggestion_helpers.go")
	require.NoError(t, err)
	body := string(source)

	require.Contains(t, body, "emptyJSONObject")
	require.NotContains(t, body, `[]byte("{}")`)
}

func TestCompanySuggestionErrorsUseCockroachErrors(t *testing.T) {
	files, err := filepath.Glob("company_suggestion_*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		require.NotContains(t, string(source), "fmt.Errorf", "%s should use github.com/cockroachdb/errors", file)
	}
}
