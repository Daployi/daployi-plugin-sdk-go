package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const LogVersion = 1
const MaxLogMessageBytes = 8 * 1024
const MaxLogFieldsBytes = 8 * 1024

// LogLevel is the severity of a plugin diagnostic, independent of telemetry.
type LogLevel string

const (
	LogTrace    LogLevel = "trace"
	LogDebug    LogLevel = "debug"
	LogInfo     LogLevel = "info"
	LogWarn     LogLevel = "warn"
	LogError    LogLevel = "error"
	LogCritical LogLevel = "critical"
)

type LogEntry struct {
	ID        string          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Level     LogLevel        `json:"level"`
	Message   string          `json:"message"`
	Fields    json.RawMessage `json:"fields,omitempty"`
}

// Accepted means validation/rate-limit admission on the agent. Console output
// still follows the agent's log level. LiveAccepted means admission to its live
// socket buffer, not server receipt or storage. False is normal when disabled,
// offline or congested. Log entries are never queued for replay.
type LogResult struct {
	Accepted     bool `json:"accepted"`
	LiveAccepted bool `json:"liveAccepted"`
}

// Log submits a diagnostic over private IPC. Supply zero or one structured fields
// map. The host attaches identity and controls console output/remote forwarding.
// Do not log passwords, tokens or entire device configurations.
func (c *Client) Log(ctx context.Context, level LogLevel, message string, fields ...map[string]any) (LogResult, error) {
	var result LogResult
	if c.logVersion != LogVersion {
		return result, &Error{Code: "unsupported_method", Message: "agent does not advertise plugin logging support"}
	}
	switch level {
	case LogTrace, LogDebug, LogInfo, LogWarn, LogError, LogCritical:
	default:
		return result, errors.New("invalid plugin log level")
	}
	if !utf8.ValidString(message) || strings.TrimSpace(message) == "" || len(message) > MaxLogMessageBytes {
		return result, errors.New("log message must be nonempty UTF-8 of at most 8 KiB")
	}
	if len(fields) > 1 {
		return result, errors.New("Log accepts at most one fields map")
	}
	entry := LogEntry{Timestamp: time.Now().UTC(), Level: level, Message: message}
	if len(fields) == 1 && fields[0] != nil {
		raw, err := json.Marshal(fields[0])
		if err != nil {
			return result, err
		}
		if len(raw) > MaxLogFieldsBytes {
			return result, errors.New("log fields exceed 8 KiB")
		}
		entry.Fields = raw
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return result, err
	}
	entry.ID = hex.EncodeToString(id[:])
	err := c.call(ctx, "log", entry, &result)
	return result, err
}
