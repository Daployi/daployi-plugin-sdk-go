package plugin

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRegisterActionMetadata(t *testing.T) {
	c := &Client{}
	h := ActionHandlerFunc(func(context.Context, Action) (any, error) { return nil, nil })
	schema := json.RawMessage(`{"type":"object","properties":{"on":{"type":"boolean"}},"required":["on"]}`)
	if err := c.RegisterAction(ActionDefinition{Name: "setLight", Title: "Set light", Description: "Set the light on or off", ParamsSchema: schema}, h); err != nil {
		t.Fatal(err)
	}
	schema[0] = 'x'
	if !json.Valid(c.actionDefinitions["setLight"].ParamsSchema) {
		t.Fatal("registration must copy caller-owned schema")
	}
	if err := c.Action("setLight", h); err == nil {
		t.Fatal("duplicate registered")
	}
	if err := c.Action("reset", h); err != nil {
		t.Fatal(err)
	}
	if c.actionDefinitions["reset"].Title != "reset" {
		t.Fatal("legacy Action must advertise name")
	}
	if err := c.RegisterAction(ActionDefinition{Name: "bad", ParamsSchema: json.RawMessage(`{"type":"array"}`)}, h); err == nil {
		t.Fatal("non-object parameters accepted")
	}
	c.actionsReady = true
	if err := c.RegisterAction(ActionDefinition{Name: "late"}, h); err == nil {
		t.Fatal("registered after Ready")
	}
}
