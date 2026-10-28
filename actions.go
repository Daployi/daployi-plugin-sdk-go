package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

const ActionVersion = 1
const MaxActionBytes = 64 * 1024
const MaxActionDuration = 30 * time.Second

// Action is a named operation requested by the agent. ID is a correlation ID,
// not a guarantee that an external device applies a write exactly once.
type Action struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Params   json.RawMessage `json:"params"`
	Deadline time.Time       `json:"deadline"`
}

// ActionHandler implements plugin-specific validation and execution. Honor ctx,
// use device I/O deadlines, and return only when the operation has completed.
// A cancelled context cannot undo a write already applied by a device.
type ActionHandler interface {
	ExecuteAction(context.Context, Action) (any, error)
}

type ActionHandlerFunc func(context.Context, Action) (any, error)

func (f ActionHandlerFunc) ExecuteAction(ctx context.Context, a Action) (any, error) {
	return f(ctx, a)
}

// Action registers an explicitly named handler before Ready. Actions execute
// serially; a second action is rejected while a handler is still running.
func (c *Client) Action(name string, handler ActionHandler) error {
	if !identifier.MatchString(name) || handler == nil {
		return errors.New("valid action name and handler required")
	}
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	if c.actionsReady {
		return errors.New("register actions before Ready")
	}
	if c.actions == nil {
		c.actions = map[string]ActionHandler{}
	}
	if len(c.actions) >= 64 {
		return errors.New("at most 64 actions may be registered")
	}
	if _, exists := c.actions[name]; exists {
		return errors.New("action already registered")
	}
	c.actions[name] = handler
	return nil
}

func (c *Client) Ready(ctx context.Context) error {
	c.actionMu.Lock()
	if c.actionsReady {
		c.actionMu.Unlock()
		return errors.New("Ready already called")
	}
	c.actionsReady = true
	names := make([]string, 0, len(c.actions))
	for name := range c.actions {
		names = append(names, name)
	}
	c.actionMu.Unlock()
	sort.Strings(names)
	return c.call(ctx, "ready", struct {
		PublishFilterVersion int      `json:"publishFilterVersion"`
		ActionVersion        int      `json:"actionVersion"`
		Actions              []string `json:"actions"`
	}{PublishFilterVersion, ActionVersion, names}, nil)
}

func (c *Client) cancelAction(id string) {
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	if c.activeAction == id && c.actionCancel != nil {
		c.actionCancel()
	}
}

// Called by the sole reader. Only the admitted handler runs asynchronously, so
// telemetry responses, ping and shutdown continue while an action is executing.
func (c *Client) receiveAction(f frame) {
	fail := func(code, message string) {
		_ = c.write(frame{Version: ProtocolVersion, ID: f.ID, Error: &Error{Code: code, Message: message}})
	}
	var a Action
	if !identifier.MatchString(f.ID) || len(f.Params) > MaxActionBytes+1024 || json.Unmarshal(f.Params, &a) != nil || a.ID != f.ID || !identifier.MatchString(a.Name) || len(a.Params) > MaxActionBytes || !json.Valid(a.Params) || !a.Deadline.After(time.Now()) || a.Deadline.After(time.Now().Add(MaxActionDuration)) {
		fail("invalid_action", "invalid action parameters or deadline")
		return
	}
	c.actionMu.Lock()
	handler := c.actions[a.Name]
	if !c.actionsReady || handler == nil {
		c.actionMu.Unlock()
		fail("unsupported_action", "action is not registered")
		return
	}
	if c.activeAction != "" {
		c.actionMu.Unlock()
		fail("busy", "another action is still running")
		return
	}
	if c.actionSeen == nil {
		c.actionSeen = map[string]time.Time{}
	}
	for id, expiry := range c.actionSeen {
		if time.Now().After(expiry) {
			delete(c.actionSeen, id)
		}
	}
	if _, exists := c.actionSeen[a.ID]; exists {
		c.actionMu.Unlock()
		fail("duplicate_action", "action ID already received")
		return
	}
	if len(c.actionSeen) >= 4096 {
		c.actionMu.Unlock()
		fail("busy", "action duplicate tracking is full")
		return
	}
	ctx, cancel := context.WithDeadline(c.ctx, a.Deadline)
	c.activeAction, c.actionCancel = a.ID, cancel
	c.actionSeen[a.ID] = a.Deadline.Add(5 * time.Minute)
	c.actionMu.Unlock()
	go func() {
		defer cancel()
		completed := make(chan frame, 1)
		go func() {
			response := frame{Version: ProtocolVersion, ID: a.ID}
			defer func() {
				if recover() != nil {
					response.Result = nil
					response.Error = &Error{Code: "action_failed", Message: "action handler panicked"}
				}
				// Keep the slot occupied until even a non-cooperative handler returns.
				c.actionMu.Lock()
				c.activeAction = ""
				c.actionCancel = nil
				c.actionMu.Unlock()
				completed <- response
			}()
			value, err := handler.ExecuteAction(ctx, a)
			if err != nil {
				response.Error = &Error{Code: "action_failed", Message: "action handler failed"}
				var public *Error
				if errors.As(err, &public) && public != nil && identifier.MatchString(public.Code) && len(public.Message) <= 1024 {
					response.Error = public
				}
				return
			}
			response.Result, err = json.Marshal(value)
			if err != nil || len(response.Result) > MaxActionBytes {
				response.Result = nil
				response.Error = &Error{Code: "invalid_result", Message: "action result must be JSON of at most 64 KiB"}
			}
		}()
		var response frame
		select {
		case response = <-completed:
		case <-ctx.Done():
			response = frame{Version: ProtocolVersion, ID: a.ID, Error: &Error{Code: "action_cancelled", Message: "action cancelled; device outcome may be unknown"}}
		}
		if c.write(response) != nil {
			c.close()
		}
	}()
}
