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

type actionHarness struct {
	enc      *json.Encoder
	messages chan frame
	done     chan error
}

func startActions(t *testing.T, setup func(*Client) error) *actionHarness {
	t.Helper()
	parentRead, childWrite := io.Pipe()
	childRead, parentWrite := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	h := &actionHarness{json.NewEncoder(parentWrite), make(chan frame, 32), make(chan error, 1)}
	go func() {
		dec := json.NewDecoder(parentRead)
		for {
			var f frame
			if dec.Decode(&f) != nil {
				return
			}
			select {
			case h.messages <- f:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		h.done <- serve(ctx, childRead, childWrite, func(ctx context.Context, c *Client, _ json.RawMessage) error {
			if err := setup(c); err != nil {
				return err
			}
			if err := c.Ready(ctx); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		})
	}()
	t.Cleanup(func() {
		cancel()
		parentRead.Close()
		parentWrite.Close()
		select {
		case <-h.done:
		case <-time.After(time.Second):
			t.Error("SDK did not stop")
		}
	})
	if err := h.enc.Encode(map[string]any{"version": 1, "method": "initialize", "params": map[string]any{"pluginId": "test", "release": "1", "config": map[string]any{}}}); err != nil {
		t.Fatal(err)
	}
	ready := h.next(t)
	if ready.Method != "ready" || !strings.Contains(string(ready.Params), `"actionVersion":1`) {
		t.Fatalf("missing action capabilities: %+v", ready)
	}
	h.send(t, frame{Version: 1, ID: ready.ID, Result: json.RawMessage(`{}`)})
	return h
}
func (h *actionHarness) next(t *testing.T) frame {
	t.Helper()
	select {
	case f := <-h.messages:
		return f
	case <-time.After(3 * time.Second):
		t.Fatal("missing RPC frame")
		return frame{}
	}
}
func (h *actionHarness) send(t *testing.T, f frame) {
	t.Helper()
	if err := h.enc.Encode(f); err != nil {
		t.Fatal(err)
	}
}
func (h *actionHarness) action(t *testing.T, id, name string, deadline time.Time) {
	t.Helper()
	raw, _ := json.Marshal(Action{id, name, json.RawMessage(`{"on":true}`), deadline})
	h.send(t, frame{Version: 1, ID: id, Method: "executeAction", Params: raw})
}

func TestActionCanPublishWhileReaderHandlesPing(t *testing.T) {
	h := startActions(t, func(c *Client) error {
		return c.Action("setLight", ActionHandlerFunc(func(ctx context.Context, a Action) (any, error) {
			r, _ := NewRecord(map[string]any{"on": true})
			if _, err := c.PublishData(ctx, "light", r); err != nil {
				return nil, err
			}
			return map[string]any{"applied": true, "count": uint64(9007199254740993)}, nil
		}))
	})
	h.action(t, "write-1", "setLight", time.Now().Add(2*time.Second))
	published := h.next(t)
	if published.Method != "publishData" {
		t.Fatalf("got %+v", published)
	}
	h.send(t, frame{Version: 1, Method: "ping"})
	if pong := h.next(t); pong.Method != "pong" {
		t.Fatalf("ping blocked by action: %+v", pong)
	}
	h.send(t, frame{Version: 1, ID: published.ID, Result: json.RawMessage(`{"acceptedIds":["ok"]}`)})
	result := h.next(t)
	if result.ID != "write-1" || result.Error != nil || !strings.Contains(string(result.Result), "9007199254740993") {
		t.Fatalf("bad action result %+v", result)
	}
	h.action(t, "write-1", "setLight", time.Now().Add(2*time.Second))
	if f := h.next(t); f.Error == nil || f.Error.Code != "duplicate_action" {
		t.Fatalf("duplicate executed: %+v", f)
	}
	h.action(t, "write-2", "absent", time.Now().Add(time.Second))
	if f := h.next(t); f.Error == nil || f.Error.Code != "unsupported_action" {
		t.Fatalf("unknown action accepted: %+v", f)
	}
}

func TestActionCancellationKeepsBusyUntilHandlerReturns(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h := startActions(t, func(c *Client) error {
		return c.Action("slow", ActionHandlerFunc(func(ctx context.Context, _ Action) (any, error) {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			<-release
			return nil, ctx.Err()
		}))
	})
	defer close(release)
	h.action(t, "slow-1", "slow", time.Now().Add(2*time.Second))
	<-entered
	h.send(t, frame{Version: 1, ID: "slow-1", Method: "cancelAction"})
	if f := h.next(t); f.Error == nil || f.Error.Code != "action_cancelled" {
		t.Fatalf("cancellation failed: %+v", f)
	}
	<-cancelled
	h.action(t, "slow-2", "slow", time.Now().Add(time.Second))
	if f := h.next(t); f.Error == nil || f.Error.Code != "busy" {
		t.Fatalf("overlapped cancelled action: %+v", f)
	}
}

func TestActionValidationPanicAndOversizedResult(t *testing.T) {
	h := startActions(t, func(c *Client) error {
		if err := c.Action("panic", ActionHandlerFunc(func(context.Context, Action) (any, error) { panic("secret") })); err != nil {
			return err
		}
		if err := c.Action("large", ActionHandlerFunc(func(context.Context, Action) (any, error) { return strings.Repeat("x", MaxActionBytes), nil })); err != nil {
			return err
		}
		if err := c.Action("error", ActionHandlerFunc(func(context.Context, Action) (any, error) { return nil, errors.New("password=secret") })); err != nil {
			return err
		}
		if c.Action("panic", ActionHandlerFunc(nil)) == nil {
			return errors.New("duplicate registration accepted")
		}
		return nil
	})
	for _, tc := range []struct{ name, code string }{{"panic", "action_failed"}, {"large", "invalid_result"}, {"error", "action_failed"}} {
		h.action(t, tc.name, tc.name, time.Now().Add(time.Second))
		f := h.next(t)
		if f.Error == nil || f.Error.Code != tc.code || strings.Contains(f.Error.Message, "secret") {
			t.Fatalf("unsafe response %+v", f)
		}
	}
	h.action(t, "expired", "panic", time.Now().Add(-time.Second))
	if f := h.next(t); f.Error == nil || f.Error.Code != "invalid_action" {
		t.Fatalf("expired action accepted %+v", f)
	}
}
