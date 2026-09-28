package resolve

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Validate refuses a record that could not be traced back or windowed:
// record_id, root_domain, name, type and value are required, and a non-empty
// first_seen/last_seen must be a date (a longer timestamp is cut to its date)
// with first_seen <= last_seen.
func (r Record) Validate() error {
	for field, v := range map[string]string{"record_id": r.RecordID, "root_domain": r.RootDomain, "name": r.Name, "type": r.Type, "value": r.Value} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	for field, v := range map[string]string{"first_seen": r.FirstSeen, "last_seen": r.LastSeen} {
		if v == "" {
			continue
		}
		if _, err := time.Parse(time.DateOnly, day(v)); err != nil {
			return fmt.Errorf("%s %q is not a date", field, v)
		}
	}
	if r.FirstSeen != "" && r.LastSeen != "" && day(r.FirstSeen) > day(r.LastSeen) {
		return errors.New("first_seen is after last_seen")
	}
	return nil
}
