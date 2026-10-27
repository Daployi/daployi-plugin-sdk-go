package main

import (
	"context"
	"encoding/json"
	"errors"
	plugin "github.com/Daployi/daployi-plugin-sdk-go"
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
		if err := c.Action("echo", plugin.ActionHandlerFunc(func(callCtx context.Context, a plugin.Action) (any, error) {
			r, err := plugin.NewRecord(map[string]any{"actionId": a.ID})
			if err != nil {
				return nil, err
			}
			if _, err = c.PublishData(callCtx, "actions", r); err != nil {
				return nil, err
			}
			return a.Params, nil
		})); err != nil {
			return err
		}
		if err := c.Action("slow", plugin.ActionHandlerFunc(func(callCtx context.Context, a plugin.Action) (any, error) {
			<-callCtx.Done()
			return nil, callCtx.Err()
		})); err != nil {
			return err
		}
		if config.Mode == "log" {
			if _, err := c.Log(ctx, plugin.LogWarn, "Plugin starting", map[string]any{"stage": "initialize"}); err != nil {
				return err
			}
		}
		if config.Mode == "change-reporting" {
			if err := c.RegisterFields("temperature", plugin.FieldDefinition{Path: "/temperature", Tolerance: 0.1}); err != nil {
				return err
			}
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
		if config.Mode == "change-reporting" {
			for i, value := range []float64{27, 27.09, 27.1} {
				sample, err := plugin.NewRecord(map[string]any{"temperature": value})
				if err != nil {
					return err
				}
				sample.Timestamp = r.Timestamp.Add(time.Duration(i+1) * time.Millisecond)
				receipt, err := c.PublishData(call, "temperature", sample)
				if err != nil {
					return err
				}
				if i < 2 && (len(receipt.SuppressedIDs) != 1 || len(receipt.AcceptedIDs) != 0) {
					return errors.New("missing suppression receipt")
				}
				if i == 2 && (len(receipt.AcceptedIDs) != 1 || len(receipt.SuppressedIDs) != 0) {
					return errors.New("missing durable receipt")
				}
			}
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
