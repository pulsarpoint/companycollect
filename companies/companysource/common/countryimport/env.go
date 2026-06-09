package countryimport

import (
	"bufio"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

func LoadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		kind := ErrorKindFileIO
		if os.IsNotExist(err) {
			kind = ErrorKindNotFound
		}
		return WrapSourceError(kind, "", "", path, 0, errors.Wrap(err, "open env file"))
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = trimSimpleQuoteWrapping(strings.TrimSpace(value))
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}

		if err := os.Setenv(key, value); err != nil {
			return WrapSourceError(ErrorKindInvalidConfig, "", "", path, 0, errors.Wrapf(err, "set env %s", key))
		}
	}

	if err := scanner.Err(); err != nil {
		return WrapSourceError(ErrorKindFileIO, "", "", path, 0, errors.Wrap(err, "scan env file"))
	}
	return nil
}

func trimSimpleQuoteWrapping(value string) string {
	if len(value) < 2 {
		return value
	}
	if value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1]
	}
	if value[0] == '"' && value[len(value)-1] == '"' {
		return value[1 : len(value)-1]
	}
	return value
}
