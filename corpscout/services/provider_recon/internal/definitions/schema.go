package definitions

import (
	"encoding/json"

	"github.com/invopop/jsonschema"
)

// Schema returns the JSON Schema for definition files. It is committed as
// definitions/schema.json for editor completion; Validate is the authority.
func Schema() ([]byte, error) {
	r := &jsonschema.Reflector{FieldNameTag: "yaml", ExpandedStruct: true}
	s := r.Reflect(&Definition{})
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
