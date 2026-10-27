package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"testing"
	"time"
)

func TestFieldRegistrationValidationAndCopy(t *testing.T) {
	c := &Client{fieldSchemaVersion: 1}
	input := []FieldDefinition{{Path: "/values/temperature", Tolerance: 0.1}}
	if err := c.RegisterFields("s", input...); err != nil {
		t.Fatal(err)
	}
	input[0].Tolerance = 100
	if c.fields["s"][0].Tolerance != 0.1 {
		t.Fatal("retained caller-owned field slice")
	}
	if err := c.RegisterFields("s", input...); err == nil {
		t.Fatal("duplicate stream accepted")
	}
	c.actionsReady = true
	if err := c.RegisterFields("next", input...); err == nil {
		t.Fatal("registration after Ready accepted")
	}
	for _, fields := range [][]FieldDefinition{
		{{Path: "bad"}}, {{Path: "/bad~2"}}, {{Path: "/x", Tolerance: -1}}, {{Path: "/x", Tolerance: math.NaN()}}, {{Path: "/x", Tolerance: math.Inf(1)}}, {{Path: "/x"}, {Path: "/x"}}, make([]FieldDefinition, 257),
	} {
		client := &Client{fieldSchemaVersion: 1}
		if err := client.RegisterFields("s", fields...); err == nil {
			t.Fatalf("invalid fields accepted: %+v", fields)
		}
	}
	old := &Client{}
	var rpc *Error
	if err := old.RegisterFields("s", input...); !errors.As(err, &rpc) || rpc.Code != "unsupported_method" {
		t.Fatalf("legacy host silently accepted fields: %v", err)
	}
}
func TestFieldRegistrationReadyWireAndSuppressedReceipt(t *testing.T) {
	parentRead, childWrite := io.Pipe()
	childRead, parentWrite := io.Pipe()
	defer parentRead.Close()
	defer parentWrite.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- serve(ctx, childRead, childWrite, func(ctx context.Context, c *Client, _ json.RawMessage) error {
			if err := c.RegisterFields("s", FieldDefinition{Path: "/values/temperature", Tolerance: 0.1}); err != nil {
				return err
			}
			if err := c.Ready(ctx); err != nil {
				return err
			}
			r, _ := NewRecord(map[string]any{"values": map[string]any{"temperature": 26}})
			receipt, err := c.PublishData(ctx, "s", r)
			if err != nil {
				return err
			}
			if len(receipt.AcceptedIDs) != 0 || len(receipt.SuppressedIDs) != 1 || receipt.SuppressedIDs[0] != r.ID {
				return errors.New("bad skipped receipt")
			}
			return nil
		})
	}()
	enc := json.NewEncoder(parentWrite)
	dec := json.NewDecoder(parentRead)
	if err := enc.Encode(map[string]any{"version": 1, "method": "initialize", "params": map[string]any{"pluginId": "test", "release": "1", "config": map[string]any{}, "fieldSchemaVersion": 1}}); err != nil {
		t.Fatal(err)
	}
	var ready frame
	if err := dec.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	var cap struct {
		FieldSchemaVersion int                          `json:"fieldSchemaVersion"`
		Fields             map[string][]FieldDefinition `json:"fields"`
	}
	if err := json.Unmarshal(ready.Params, &cap); err != nil || ready.Method != "ready" || cap.FieldSchemaVersion != 1 || len(cap.Fields["s"]) != 1 || cap.Fields["s"][0].Tolerance != 0.1 {
		t.Fatalf("bad field handshake: %s %v", ready.Params, err)
	}
	enc.Encode(map[string]any{"version": 1, "id": ready.ID, "result": map[string]any{"ready": true}})
	var publish frame
	if err := dec.Decode(&publish); err != nil {
		t.Fatal(err)
	}
	var data data
	if err := json.Unmarshal(publish.Params, &data); err != nil {
		t.Fatal(err)
	}
	enc.Encode(map[string]any{"version": 1, "id": publish.ID, "result": map[string]any{"acceptedIds": []string{}, "suppressedIds": []string{data.Records[0].ID}}})
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
