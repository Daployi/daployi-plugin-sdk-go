package main

import (
	"context"
	plugin "daployi-plugin-sdk-go"
	"encoding/json"
	"os"
	"time"
)

func main() {
	err := plugin.Run(context.Background(), func(ctx context.Context, c *plugin.Client, raw json.RawMessage) error {
		var config struct {
			Mode string `json:"mode"`
		}
		json.Unmarshal(raw, &config)
		if config.Mode == "not-ready" {
			<-ctx.Done()
			return nil
		}
		if err := c.Ready(ctx); err != nil {
			return err
		}
		if config.Mode == "crash" {
			os.Exit(7)
		}
		r, err := plugin.NewRecord(map[string]any{"temperature": 27})
		if err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err = c.PublishData(call, "temperature", r); err != nil {
			return err
		}
		if _, err = c.ReviewData(call, "temperature", r); err != nil {
			return err
		}
		if _, err = c.PushLiveData(call, "temperature", r); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
	if err != nil {
		os.Exit(1)
	}
}
