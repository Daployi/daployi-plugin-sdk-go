package plugin

import (
	"encoding/json"
	"errors"
	"math"
)

const FieldSchemaVersion = 1

// FieldDefinition describes a published payload field, not a device register.
// Path is a JSON Pointer relative to payload. Tolerance is absolute, in the
// published field's units after any scaling. Omitted tolerance means zero.
type FieldDefinition struct {
	Path      string  `json:"path"`
	Tolerance float64 `json:"tolerance,omitempty"`
}

// RegisterFields declares one stream's fields before Ready. Configuration stays
// in the plugin's field definitions; the plugin derives paths automatically.
// Registration does not enable change reporting; that remains an agent assignment
// policy. The host receives all registrations atomically in Ready.
func (c *Client) RegisterFields(stream string, fields ...FieldDefinition) error {
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	if c.actionsReady {
		return errors.New("register fields before Ready")
	}
	if c.fieldSchemaVersion != FieldSchemaVersion {
		return &Error{Code: "unsupported_method", Message: "agent does not support field registration; update the agent"}
	}
	if !identifier.MatchString(stream) || len(fields) > 256 {
		return errors.New("invalid stream or more than 256 fields")
	}
	if _, exists := c.fields[stream]; exists {
		return errors.New("stream fields already registered")
	}
	if len(c.fields) >= 128 {
		return errors.New("at most 128 field streams")
	}
	seen := map[string]bool{}
	for _, f := range fields {
		if !validFieldPath(f.Path) || seen[f.Path] || f.Tolerance < 0 || math.IsNaN(f.Tolerance) || math.IsInf(f.Tolerance, 0) {
			return errors.New("fields require unique JSON Pointer paths and finite nonnegative tolerances")
		}
		seen[f.Path] = true
	}
	next := make(map[string][]FieldDefinition, len(c.fields)+1)
	for key, value := range c.fields {
		next[key] = value
	}
	next[stream] = append([]FieldDefinition{}, fields...)
	raw, err := json.Marshal(next)
	if err != nil || len(raw) > 64*1024 {
		return errors.New("field registration exceeds 64 KiB")
	}
	c.fields = next
	return nil
}
func validFieldPath(path string) bool {
	if path == "" {
		return true
	}
	if len(path) > 512 || path[0] != '/' {
		return false
	}
	for i := 0; i < len(path); i++ {
		if path[i] == '~' {
			i++
			if i == len(path) || (path[i] != '0' && path[i] != '1') {
				return false
			}
		}
	}
	return true
}
