package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/cockroachdb/errors"
)

var forbiddenConfigKey = regexp.MustCompile(`(?i)(key|secret|token|password)`)

func validateConfigPatch(config map[string]json.RawMessage) error {
	for key, value := range config {
		if forbiddenConfigKey.MatchString(key) {
			return errors.Newf("forbidden config key %q", key)
		}
		if !json.Valid(value) {
			return errors.Newf("invalid json for config key %q", key)
		}
		var decoded any
		dec := json.NewDecoder(bytes.NewReader(value))
		dec.UseNumber()
		if err := dec.Decode(&decoded); err != nil {
			return errors.Wrapf(err, "decode config key %q", key)
		}
		if err := validateNestedConfigKeys(key, decoded); err != nil {
			return errors.Wrapf(err, "validate config key %q", key)
		}
	}
	return nil
}

func validateNestedConfigKeys(path string, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, nestedValue := range typed {
			nestedPath := path + "." + key
			if forbiddenConfigKey.MatchString(key) {
				return errors.Newf("forbidden nested config key %q", nestedPath)
			}
			if err := validateNestedConfigKeys(nestedPath, nestedValue); err != nil {
				return errors.Wrapf(err, "validate nested config key %q", nestedPath)
			}
		}
	case []any:
		for i, nestedValue := range typed {
			nestedPath := fmt.Sprintf("%s[%d]", path, i)
			if err := validateNestedConfigKeys(nestedPath, nestedValue); err != nil {
				return errors.Wrapf(err, "validate nested config item %q", nestedPath)
			}
		}
	}
	return nil
}

func mergeConfig(existing json.RawMessage, patch map[string]json.RawMessage) (json.RawMessage, error) {
	merged := map[string]json.RawMessage{}
	if len(bytes.TrimSpace(existing)) > 0 && string(bytes.TrimSpace(existing)) != "null" {
		if err := json.Unmarshal(existing, &merged); err != nil {
			return nil, errors.Wrap(err, "decode existing source config")
		}
	}
	for key, value := range patch {
		copied := make(json.RawMessage, len(value))
		copy(copied, value)
		merged[key] = copied
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return nil, errors.Wrap(err, "encode merged source config")
	}
	return json.RawMessage(out), nil
}
