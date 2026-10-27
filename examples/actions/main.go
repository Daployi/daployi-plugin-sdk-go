// This example simulates a light. Replace the state update with a bounded device
// write in your own plugin; no GPIO or Modbus hardware is controlled here.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync/atomic"

	plugin "github.com/Daployi/daployi-plugin-sdk-go"
)

type light struct{ on atomic.Bool }

func (l *light) ExecuteAction(ctx context.Context, action plugin.Action) (any, error) {
	var params struct {
		On *bool `json:"on"`
	}
	if json.Unmarshal(action.Params, &params) != nil || params.On == nil {
		return nil, &plugin.Error{Code: "invalid_params", Message: "on must be an explicit boolean"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Prefer explicit target state (set on/off) over non-idempotent toggle commands.
	// Real integrations must validate address/range against assigned configuration
	// and use the action context/deadline for all device I/O.
	l.on.Store(*params.On)
	return map[string]any{"on": l.on.Load()}, nil
}

func main() {
	logger := log.New(os.Stderr, "actions: ", log.LstdFlags)
	err := plugin.Run(context.Background(), func(ctx context.Context, c *plugin.Client, _ json.RawMessage) error {
		if err := c.Action("setLight", &light{}); err != nil {
			return err
		}
		if err := c.Ready(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
	if err != nil {
		logger.Print(err)
		os.Exit(1)
	}
}
