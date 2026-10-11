// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"bytes"
	"encoding/json"
)

// anthropicUsage is the usage object of the Messages API.
type anthropicUsage struct {
	InputTokens              uint64 `json:"input_tokens"`
	OutputTokens             uint64 `json:"output_tokens"`
	CacheCreationInputTokens uint64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     uint64 `json:"cache_read_input_tokens"`
}

func (u anthropicUsage) normalise() upstreamUsage {
	return upstreamUsage{
		input: u.InputTokens, output: u.OutputTokens,
		cacheRead: u.CacheReadInputTokens, cacheWrite: u.CacheCreationInputTokens,
	}
}

// cacheableStop says whether an answer with this stop reason is a finished answer. A refusal, or a
// turn paused for a server-side tool, is not something to serve again.
func cacheableStop(reason string) bool {
	switch reason {
	case "end_turn", "max_tokens", "stop_sequence", "tool_use":
		return true
	}
	return false
}

// inspectMessages decides whether a non-streaming Messages API answer is worth keeping.
func inspectMessages(body []byte) (jsonInfo, bool) {
	var v struct {
		Type       string          `json:"type"`
		Model      string          `json:"model"`
		StopReason *string         `json:"stop_reason"`
		Error      json.RawMessage `json:"error"`
		Usage      anthropicUsage  `json:"usage"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return jsonInfo{}, false
	}
	if v.Type != "message" || (len(v.Error) > 0 && string(v.Error) != "null") {
		return jsonInfo{}, false
	}
	if v.StopReason == nil || !cacheableStop(*v.StopReason) {
		return jsonInfo{}, false
	}
	return infoFromUsage(v.Model, v.Usage.normalise()), true
}

// anthropicStream is what a captured Messages API event stream contains.
type anthropicStream struct {
	done       bool // saw message_stop
	bad        bool // an error event or a line that is not an event
	model      string
	stopReason string
	usage      upstreamUsage
}

func (s *anthropicStream) complete() bool {
	return s.done && !s.bad && cacheableStop(s.stopReason)
}

// parseAnthropicStream reads the events of a Messages API stream. Each event is an `event:` line and a
// `data:` line whose JSON has a `type`. message_start carries the model and the input-side usage,
// message_delta carries the stop reason and the output tokens (cumulative), message_stop ends the stream.
func parseAnthropicStream(body []byte) *anthropicStream {
	s := &anthropicStream{}
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		var ev struct {
			Type    string `json:"type"`
			Message struct {
				Model string         `json:"model"`
				Usage anthropicUsage `json:"usage"`
			} `json:"message"`
			Delta struct {
				StopReason *string `json:"stop_reason"`
			} `json:"delta"`
			Usage *anthropicUsage `json:"usage"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(line[len("data:"):]), &ev); err != nil {
			s.bad = true
			continue
		}
		switch ev.Type {
		case "message_start":
			s.model = ev.Message.Model
			s.usage = ev.Message.Usage.normalise()
		case "message_delta":
			if ev.Delta.StopReason != nil {
				s.stopReason = *ev.Delta.StopReason
			}
			if ev.Usage != nil { // cumulative: the latest value of each field wins
				u := ev.Usage.normalise()
				if u.output > 0 {
					s.usage.output = u.output
				}
				if u.input > 0 {
					s.usage.input = u.input
				}
				if u.cacheRead > 0 {
					s.usage.cacheRead = u.cacheRead
				}
				if u.cacheWrite > 0 {
					s.usage.cacheWrite = u.cacheWrite
				}
			}
		case "message_stop":
			s.done = true
		case "error":
			s.bad = true
		}
	}
	return s
}

// hasCacheControl reports whether a request already says where the provider should cache.
func hasCacheControl(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		if _, has := t["cache_control"]; has {
			return true
		}
		for _, child := range t {
			if hasCacheControl(child) {
				return true
			}
		}
	case []any:
		for _, child := range t {
			if hasCacheControl(child) {
				return true
			}
		}
	}
	return false
}

// withAutoCache adds Anthropic's automatic prompt caching to a Messages API request that does not use
// prompt caching yet: a top-level cache_control, which places the breakpoint on the last cacheable block
// and moves it forward as a conversation grows. A request that already has any cache_control is left alone.
func withAutoCache(body []byte, ttl string) ([]byte, bool) {
	req, err := parseBody(body)
	if err != nil || hasCacheControl(req) {
		return body, false
	}
	cc := map[string]any{"type": "ephemeral"}
	if ttl == "1h" {
		cc["ttl"] = "1h"
	}
	req["cache_control"] = cc
	out, err := json.Marshal(req)
	if err != nil {
		return body, false
	}
	return out, true
}
