package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

func TestAllMethodsAndShutdown(t *testing.T) {
	parentRead, childWrite := io.Pipe()
	childRead, parentWrite := io.Pipe()
	defer parentRead.Close()
	defer parentWrite.Close()
	result := make(chan error, 1)
	methodsDone := make(chan struct{})
	go func() {
		result <- serve(context.Background(), childRead, childWrite, func(ctx context.Context, c *Client, cfg json.RawMessage) error {
			if c.PluginID != "test" || string(cfg) != "{}" {
				return errors.New("bad handshake")
			}
			if err := c.Ready(ctx); err != nil {
				return err
			}
			r, err := NewRecord(map[string]any{"v": 27})
			if err != nil {
				return err
			}
			receipt, err := c.PublishData(ctx, "temperature", r)
			if err != nil {
				return err
			}
			if len(receipt.AcceptedIDs) != 1 || receipt.AcceptedIDs[0] != r.ID {
				return errors.New("bad receipt")
			}
			review, err := c.ReviewData(ctx, "temperature", r)
			if err != nil || review.Evaluated != 2 {
				return errors.New("bad review")
			}
			live, err := c.PushLiveData(ctx, "temperature", r)
			if err != nil || live.Accepted {
				return errors.New("offline live should be dropped")
			}
			close(methodsDone)
			<-ctx.Done()
			return nil
		})
	}()
	enc := json.NewEncoder(parentWrite)
	dec := json.NewDecoder(parentRead)
	enc.Encode(map[string]any{"version": 1, "method": "initialize", "params": map[string]any{"pluginId": "test", "release": "1", "config": map[string]any{}}})
	for _, method := range []string{"ready", "publishData", "reviewData", "pushLiveData"} {
		var f frame
		if err := dec.Decode(&f); err != nil {
			t.Fatal(err)
		}
		if f.Method != method {
			t.Fatalf("method %s", f.Method)
		}
		if method == "ready" {
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(f.Params, &raw); err != nil {
				t.Fatal(err)
			}
			if raw["fields"] != nil || raw["fieldSchemaVersion"] != nil {
				t.Fatal("ready contains removed field registration")
			}
			var capabilities struct {
				PublishFilterVersion int `json:"publishFilterVersion"`
			}
			if err := json.Unmarshal(f.Params, &capabilities); err != nil || capabilities.PublishFilterVersion != 1 {
				t.Fatal("missing filtered-receipt capability")
			}
		}
		var data data
		_ = json.Unmarshal(f.Params, &data)
		var res any = map[string]any{}
		switch method {
		case "publishData":
			res = Receipt{AcceptedIDs: []string{data.Records[0].ID}}
		case "reviewData":
			res = ReviewResult{2}
		case "pushLiveData":
			res = LiveResult{false}
		}
		enc.Encode(map[string]any{"version": 1, "id": f.ID, "result": res})
	}
	select {
	case <-methodsDone:
	case err := <-result:
		t.Fatalf("methods failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("methods timed out")
	}
	enc.Encode(map[string]any{"version": 1, "method": "shutdown"})
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown timed out")
	}
}
func TestMalformedHandshake(t *testing.T) {
	in, out := io.Pipe()
	go func() { out.Write([]byte(`{"version":999,"method":"initialize"}` + "\n")); out.Close() }()
	if err := serve(context.Background(), in, io.Discard, func(context.Context, *Client, json.RawMessage) error { return nil }); err == nil {
		t.Fatal("accepted incompatible host")
	}
}
func TestRecordValidation(t *testing.T) {
	if err := validate("", nil); err == nil {
		t.Fatal("accepted empty data")
	}
	r, _ := NewRecord(map[string]any{"v": 1})
	if err := validate("s", []Record{r}); err != nil {
		t.Fatal(err)
	}
	r.Payload = json.RawMessage(`{`)
	if err := validate("s", []Record{r}); err == nil {
		t.Fatal("accepted malformed JSON")
	}
}

func TestCanceledCallClosesBlockedPipe(t *testing.T) {
	parentRead, childWrite := io.Pipe()
	childRead, parentWrite := io.Pipe()
	defer parentRead.Close()
	defer parentWrite.Close()
	result := make(chan error, 1)
	go func() {
		result <- serve(context.Background(), childRead, childWrite, func(ctx context.Context, c *Client, _ json.RawMessage) error {
			callCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()
			return c.Ready(callCtx)
		})
	}()
	json.NewEncoder(parentWrite).Encode(map[string]any{"version": 1, "method": "initialize", "params": map[string]any{"pluginId": "test", "release": "1", "config": map[string]any{}}})
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked parent ignored cancellation")
	}
	if _, err := childWrite.Write([]byte("closed")); err == nil {
		t.Fatal("writer remained open")
	}
}
