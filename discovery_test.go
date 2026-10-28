package plugin

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestDiscoveryValidation(t *testing.T) {
	f := DiscoveredField{ID: "probe", Stream: "plant", Path: "/tags/temperature", Name: "Probe temperature", Type: "number", Unit: "°C"}
	if err := validateDiscoveredFields([]DiscoveredField{f}); err != nil {
		t.Fatal(err)
	}
	if err := validateDiscoveredFields([]DiscoveredField{f, f}); err == nil {
		t.Fatal("duplicate accepted")
	}
	f.Path = "/bad~2"
	if err := validateDiscoveredFields([]DiscoveredField{f}); err == nil {
		t.Fatal("invalid pointer accepted")
	}
	c := &Client{}
	if err := c.RegisterFields(context.Background()); err == nil {
		t.Fatal("unsupported agent accepted")
	}
}

func TestDiscoveryTwoThousandFields(t *testing.T) {
	fields := make([]DiscoveredField, 2000)
	for i := range fields {
		fields[i] = DiscoveredField{ID: fmt.Sprintf("tag-%d", i), Stream: "plant", Path: fmt.Sprintf("/values/%d", i), Name: "Tag", Type: "number", Description: strings.Repeat("x", 512)}
	}
	if err := validateDiscoveredFields(fields); err != nil {
		t.Fatal(err)
	}
	fields = append(fields, DiscoveredField{ID: "extra", Stream: "plant", Path: "/extra", Name: "Extra", Type: "number"})
	if err := validateDiscoveredFields(fields); err == nil {
		t.Fatal("2001 fields accepted")
	}
}
