package main

import (
	"context"
	"encoding/json"
	plugin "github.com/Daployi/daployi-plugin-sdk-go"
	"log"
	"os"
	"time"
)

func main() {
	err := plugin.Run(context.Background(), func(ctx context.Context, c *plugin.Client, _ json.RawMessage) error {
		// A real protocol driver builds this list from browsed device tags.
		if err := c.RegisterFields(ctx, plugin.DiscoveredField{ID: "probe-1", Stream: "plant", Path: "/tags/temperature", Name: "Probe temperature", Type: "number", Unit: "°C"}); err != nil {
			return err
		}
		if err := c.Ready(ctx); err != nil {
			return err
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				record, err := plugin.NewRecord(map[string]any{"tags": map[string]any{"temperature": 26.5}})
				if err != nil {
					return err
				}
				if _, err = c.PublishData(ctx, "plant", record); err != nil {
					return err
				}
			}
		}
	})
	if err != nil {
		log.New(os.Stderr, "", log.LstdFlags).Fatal(err)
	}
}
