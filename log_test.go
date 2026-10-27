package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestLogBeforeReadyRoundTrip(t *testing.T) {
	parentRead, childWrite := io.Pipe()
	childRead, parentWrite := io.Pipe()
	defer parentRead.Close()
	defer parentWrite.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- serve(ctx, childRead, childWrite, func(ctx context.Context, c *Client, _ json.RawMessage) error {
			receipt, err := c.Log(ctx, LogInfo, "Connected to Modbus", map[string]any{"unitId": 1})
			if err != nil {
				return err
			}
			if !receipt.Accepted || receipt.LiveAccepted {
				return errors.New("unexpected log receipt")
			}
			if err = c.Ready(ctx); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		})
	}()
	encoder := json.NewEncoder(parentWrite)
	decoder := json.NewDecoder(parentRead)
	if err := encoder.Encode(map[string]any{"version": 1, "method": "initialize", "params": map[string]any{"pluginId": "modbus", "release": "1", "config": map[string]any{}, "logVersion": 1}}); err != nil {
		t.Fatal(err)
	}
	var f frame
	if err := decoder.Decode(&f); err != nil {
		t.Fatal(err)
	}
	var entry LogEntry
	if json.Unmarshal(f.Params, &entry) != nil || f.Method != "log" || entry.Level != LogInfo || entry.ID == "" || entry.Timestamp.IsZero() || entry.Message != "Connected to Modbus" || string(entry.Fields) != `{"unitId":1}` {
		t.Fatalf("invalid log frame: %+v", f)
	}
	encoder.Encode(map[string]any{"version": 1, "id": f.ID, "result": LogResult{Accepted: true}})
	if err := decoder.Decode(&f); err != nil || f.Method != "ready" {
		t.Fatalf("log broke readiness: %+v %v", f, err)
	}
	encoder.Encode(map[string]any{"version": 1, "id": f.ID, "result": map[string]any{}})
	encoder.Encode(map[string]any{"version": 1, "method": "shutdown"})
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("log session hung")
	}
}

func TestLogValidationAndLegacyHost(t *testing.T) {
	c := &Client{}
	_, err := c.Log(context.Background(), LogInfo, "test")
	var remote *Error
	if !errors.As(err, &remote) || remote.Code != "unsupported_method" {
		t.Fatalf("legacy host was contacted: %v", err)
	}
	c.logVersion = LogVersion
	for _, tc := range []struct {
		level   LogLevel
		message string
		fields  []map[string]any
	}{
		{"invalid", "test", nil}, {LogInfo, " ", nil}, {LogInfo, strings.Repeat("x", MaxLogMessageBytes+1), nil}, {LogInfo, string([]byte{0xff}), nil},
		{LogInfo, "test", []map[string]any{{"secret": strings.Repeat("x", MaxLogFieldsBytes)}}},
		{LogInfo, "test", []map[string]any{{"invalid": make(chan int)}}},
		{LogInfo, "test", []map[string]any{{}, {}}},
	} {
		if _, err := c.Log(context.Background(), tc.level, tc.message, tc.fields...); err == nil {
			t.Fatal("invalid log accepted")
		}
	}
}
