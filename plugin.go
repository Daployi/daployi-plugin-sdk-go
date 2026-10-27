// Package plugin connects a native plugin subprocess to its parent Deploy agent.
// Standard input and standard output are reserved for the versioned RPC protocol;
// write diagnostic logs to standard error. No server credentials are required.
package plugin

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

const ProtocolVersion = 1
const MaxFrameBytes = 1024 * 1024
const MaxRecords = 100

// PublishFilterVersion advertises support for separately acknowledged skipped records.
const PublishFilterVersion = 1

// Record IDs must remain unchanged when retrying PublishData after an uncertain
// outcome. Consult Receipt: accepted records are durable on the agent, while
// suppressed records were intentionally not stored. Neither implies server ingestion.
type Record struct {
	ID        string          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func NewRecord(payload any) (Record, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Record{}, err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return Record{}, err
	}
	return Record{ID: hex.EncodeToString(id[:]), Timestamp: time.Now().UTC(), Payload: b}, nil
}

type Receipt struct {
	// AcceptedIDs identifies records accepted into the durable agent queue.
	AcceptedIDs []string `json:"acceptedIds"`
	// SuppressedIDs identifies unchanged readings handled without storage. Do not retry them.
	SuppressedIDs []string `json:"suppressedIds,omitempty"`
}
type ReviewResult struct {
	Evaluated int `json:"evaluated"`
}

// LiveResult reports admission to the current socket's bounded memory buffer.
// It is not a delivery acknowledgment. Live records are never retried or stored.
type LiveResult struct {
	Accepted bool `json:"accepted"`
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

var ErrClosed = errors.New("plugin agent connection closed")

type frame struct {
	Version int             `json:"version"`
	ID      string          `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}
type initialization struct {
	FieldSchemaVersion int             `json:"fieldSchemaVersion,omitempty"`
	LogVersion         int             `json:"logVersion,omitempty"`
	PluginID           string          `json:"pluginId"`
	Release            string          `json:"release"`
	Config             json.RawMessage `json:"config"`
}
type data struct {
	Stream  string   `json:"stream"`
	Records []Record `json:"records"`
}

type Client struct {
	fieldSchemaVersion int
	fields             map[string][]FieldDefinition
	logVersion         int
	ctx                context.Context
	actionMu           sync.Mutex
	actions            map[string]ActionHandler
	actionsReady       bool
	activeAction       string
	actionCancel       context.CancelFunc
	actionSeen         map[string]time.Time
	PluginID           string
	Release            string
	writer             io.Writer
	reader             io.Reader
	writeMu            sync.Mutex
	mu                 sync.Mutex
	pending            map[string]chan frame
	sequence           atomic.Uint64
	done               chan struct{}
	stopOnce           sync.Once
	cancel             context.CancelFunc
}

// Run waits for the parent handshake, then calls handler with the assigned
// configuration. Call Ready after initialization succeeds. Returning ends the
// process session; cancellation requests graceful cleanup in handler.
func Run(ctx context.Context, handler func(context.Context, *Client, json.RawMessage) error) error {
	return serve(ctx, os.Stdin, os.Stdout, handler)
}
func serve(ctx context.Context, in io.Reader, out io.Writer, handler func(context.Context, *Client, json.RawMessage) error) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), MaxFrameBytes)
	if !scanner.Scan() {
		if scanner.Err() != nil {
			return scanner.Err()
		}
		return ErrClosed
	}
	var first frame
	if err := json.Unmarshal(scanner.Bytes(), &first); err != nil {
		return err
	}
	if first.Version != ProtocolVersion || first.Method != "initialize" {
		return errors.New("unsupported agent handshake")
	}
	var init initialization
	if err := json.Unmarshal(first.Params, &init); err != nil {
		return err
	}
	if init.PluginID == "" || init.Release == "" || !json.Valid(init.Config) {
		return errors.New("invalid agent initialization")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &Client{fieldSchemaVersion: init.FieldSchemaVersion, logVersion: init.LogVersion, ctx: ctx, PluginID: init.PluginID, Release: init.Release, writer: out, reader: in, pending: make(map[string]chan frame), done: make(chan struct{}), cancel: cancel}
	defer c.close()
	go c.read(scanner)
	go func() {
		select {
		case <-ctx.Done():
			c.close()
		case <-c.done:
		}
	}()
	return handler(ctx, c, init.Config)
}
func (c *Client) close() {
	c.stopOnce.Do(func() {
		close(c.done)
		c.cancel()
		// Closing the owned pipes releases blocked readers and writers without
		// waiting for writeMu, which a blocked write may hold.
		if v, ok := c.reader.(io.Closer); ok {
			_ = v.Close()
		}
		if v, ok := c.writer.(io.Closer); ok {
			_ = v.Close()
		}
	})
}
func (c *Client) read(scanner *bufio.Scanner) {
	defer c.close()
	for scanner.Scan() {
		var f frame
		if json.Unmarshal(scanner.Bytes(), &f) != nil || f.Version != ProtocolVersion {
			return
		}
		switch f.Method {
		case "executeAction":
			c.receiveAction(f)
			continue
		case "cancelAction":
			c.cancelAction(f.ID)
			continue
		case "shutdown":
			return
		case "ping":
			if c.write(frame{Version: ProtocolVersion, Method: "pong"}) != nil {
				return
			}
			continue
		case "":
		default:
			return
		}
		c.mu.Lock()
		ch := c.pending[f.ID]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- f:
			default:
			}
		}
	}
}
func (c *Client) write(f frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(b)+1 >= MaxFrameBytes {
		return errors.New("RPC frame too large")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.done:
		return ErrClosed
	default:
	}
	_, err = c.writer.Write(append(b, '\n'))
	return err
}
func (c *Client) call(ctx context.Context, method string, params any, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := json.Marshal(params)
	if err != nil {
		return err
	}
	id := fmt.Sprint(c.sequence.Add(1))
	ch := make(chan frame, 1)
	c.mu.Lock()
	if len(c.pending) >= 16 {
		c.mu.Unlock()
		return &Error{Code: "busy", Message: "too many concurrent calls"}
	}
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	// A blocked parent pipe must not prevent the caller observing cancellation.
	// At most 16 calls can be in flight; a failed connection cancels the handler.
	written := make(chan error, 1)
	go func() { written <- c.write(frame{Version: ProtocolVersion, ID: id, Method: method, Params: b}) }()
	select {
	case err = <-written:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		c.close()
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
	select {
	case f := <-ch:
		if f.Error != nil {
			return f.Error
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(f.Result, result)
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}
func validate(stream string, records []Record) error {
	if !identifier.MatchString(stream) {
		return errors.New("stream must be an identifier of 1 to 128 ASCII letters, numbers, dots, underscores or hyphens")
	}
	if len(records) == 0 || len(records) > MaxRecords {
		return errors.New("expected 1 to 100 records")
	}
	for _, r := range records {
		if !identifier.MatchString(r.ID) || r.Timestamp.IsZero() || !json.Valid(r.Payload) {
			return errors.New("record requires ID, timestamp and JSON payload")
		}
	}
	return nil
}
func (c *Client) PublishData(ctx context.Context, stream string, records ...Record) (Receipt, error) {
	var r Receipt
	if err := validate(stream, records); err != nil {
		return r, err
	}
	err := c.call(ctx, "publishData", data{stream, records}, &r)
	return r, err
}

// ReviewData evaluates configured plugin alert rules, without persisting a
// measurement. Old records are rejected by the host to avoid historical alarms.
func (c *Client) ReviewData(ctx context.Context, stream string, record Record) (ReviewResult, error) {
	var r ReviewResult
	if err := validate(stream, []Record{record}); err != nil {
		return r, err
	}
	err := c.call(ctx, "reviewData", data{stream, []Record{record}}, &r)
	return r, err
}
func (c *Client) PushLiveData(ctx context.Context, stream string, records ...Record) (LiveResult, error) {
	var r LiveResult
	if err := validate(stream, records); err != nil {
		return r, err
	}
	err := c.call(ctx, "pushLiveData", data{stream, records}, &r)
	return r, err
}
