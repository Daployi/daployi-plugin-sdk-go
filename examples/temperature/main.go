package main

import (
	"context"
	plugin "daployi-plugin-sdk-go"
	"encoding/json"
	"log"
	"os"
	"time"
)

func main() {
	log.SetOutput(os.Stderr)
	err := plugin.Run(context.Background(), func(ctx context.Context, c *plugin.Client, raw json.RawMessage) error {
		cfg := struct {
			IntervalMS int `json:"intervalMs"`
		}{1000}
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
		if cfg.IntervalMS < 10 {
			cfg.IntervalMS = 10
		}
		if err := c.Ready(ctx); err != nil {
			return err
		}
		ticker := time.NewTicker(time.Duration(cfg.IntervalMS) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				record, err := plugin.NewRecord(map[string]any{"temperature": 27.0})
				if err != nil {
					return err
				}
				call, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err = c.PublishData(call, "temperature", record)
				cancel()
				if err != nil {
					log.Printf("publish: %v", err)
				}
				// Opt in separately to ReviewData or PushLiveData when required. Publishing
				// a historical measurement does not automatically trigger either operation.
			}
		}
	})
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
