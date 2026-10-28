package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const FieldDiscoveryVersion = 1

// Limits apply to the complete metadata snapshot, not the number of payload values.
const MaxDiscoveredFields = 2000
const MaxDiscoveryBytes = 8 * 1024 * 1024

// DiscoveredField describes an original published payload value, never agent policy.
// ID must remain stable when labels or source paths change. Unit is optional.
type DiscoveredField struct {
	ID          string `json:"id"`
	Stream      string `json:"stream"`
	Path        string `json:"path"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Unit        string `json:"unit,omitempty"`
	Description string `json:"description,omitempty"`
}

func validateDiscoveredFields(fields []DiscoveredField) error {
	if len(fields) > MaxDiscoveredFields {
		return errors.New("at most 2000 discovered fields")
	}
	seen := map[string]bool{}
	for _, f := range fields {
		if !identifier.MatchString(f.ID) || !identifier.MatchString(f.Stream) || seen[f.ID] {
			return errors.New("unique field ID and valid stream required")
		}
		seen[f.ID] = true
		if len(f.Path) > 512 || (f.Path != "" && !strings.HasPrefix(f.Path, "/")) || strings.ContainsAny(f.Path, "\x00") {
			return errors.New("invalid field JSON Pointer")
		}
		for i := 0; i < len(f.Path); i++ {
			if f.Path[i] == '~' {
				i++
				if i >= len(f.Path) || (f.Path[i] != '0' && f.Path[i] != '1') {
					return errors.New("invalid JSON Pointer escape")
				}
			}
		}
		if !utf8.ValidString(f.Name) || strings.TrimSpace(f.Name) == "" || len(f.Name) > 128 || !utf8.ValidString(f.Description) || len(f.Description) > 512 || !utf8.ValidString(f.Unit) || len(f.Unit) > 64 {
			return errors.New("invalid field labels")
		}
		switch f.Type {
		case "number", "integer", "boolean", "string", "object", "array":
		default:
			return errors.New("invalid field type")
		}
	}
	b, err := json.Marshal(fields)
	if err != nil || len(b) > MaxDiscoveryBytes {
		return errors.New("field discovery exceeds 8 MiB")
	}
	return nil
}

// RegisterFields replaces this plugin's entire discovered-field snapshot. Call at
// startup (before or after Ready) and on definition changes, not on every reading.
// An empty snapshot withdraws all discoveries. A successful return means durable
// acceptance on the agent; delivery to Deploy is retried after reconnect.
func (c *Client) RegisterFields(ctx context.Context, fields ...DiscoveredField) error {
	if c.fieldDiscoveryVersion != FieldDiscoveryVersion {
		return &Error{Code: "unsupported_method", Message: "agent does not support field discovery"}
	}
	if err := validateDiscoveredFields(fields); err != nil {
		return err
	}
	if fields == nil {
		fields = []DiscoveredField{}
	}
	return c.call(ctx, "registerFields", struct {
		Fields []DiscoveredField `json:"fields"`
	}{fields}, nil)
}
