// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
)

// jsonInfo is what the gateway needs to know about an upstream answer.
type jsonInfo struct {
	model            string
	promptTokens     uint64
	completionTokens uint64
	plainText        bool   // one plain-text answer (no tool calls), which the semantic cache may keep
	text             string // that answer
}

type usageJSON struct {
	PromptTokens     uint64 `json:"prompt_tokens"`
	CompletionTokens uint64 `json:"completion_tokens"`
}

// inspectJSON decides whether a non-streaming answer is worth keeping.
// Errors, truncated-by-filter answers and anything unexpected are not.
func inspectJSON(k kind, body []byte) (jsonInfo, bool) {
	var v struct {
		Model   string          `json:"model"`
		Error   json.RawMessage `json:"error"`
		Usage   usageJSON       `json:"usage"`
		Data    []json.RawMessage
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
			Text         string  `json:"text"`
			Message      struct {
				Content      json.RawMessage `json:"content"`
				ToolCalls    json.RawMessage `json:"tool_calls"`
				FunctionCall json.RawMessage `json:"function_call"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return jsonInfo{}, false
	}
	if len(v.Error) > 0 && string(v.Error) != "null" {
		return jsonInfo{}, false
	}
	info := jsonInfo{model: v.Model, promptTokens: v.Usage.PromptTokens, completionTokens: v.Usage.CompletionTokens}
	if k == kindEmbeddings {
		return info, len(v.Data) > 0
	}
	if len(v.Choices) == 0 {
		return jsonInfo{}, false
	}
	for _, c := range v.Choices {
		if c.FinishReason == nil || *c.FinishReason == "content_filter" {
			return jsonInfo{}, false
		}
	}
	if k == kindChat && len(v.Choices) == 1 {
		m := v.Choices[0].Message
		var text string
		if empty(m.ToolCalls) && empty(m.FunctionCall) && json.Unmarshal(m.Content, &text) == nil && text != "" {
			info.plainText, info.text = true, text
		}
	}
	return info, true
}

func empty(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null" || s == "[]"
}

// streamInfo is what a captured server-sent event stream contains.
type streamInfo struct {
	done             bool // saw data: [DONE]
	bad              bool // a line that is not a normal chunk, such as an error
	model            string
	finish           string
	hasFinish        bool
	toolCalls        bool
	content          strings.Builder
	promptTokens     uint64
	completionTokens uint64
}

func (s *streamInfo) finished() bool {
	return !s.bad && s.hasFinish && s.finish != "content_filter"
}

func (s *streamInfo) plainText() bool { return !s.toolCalls && s.content.Len() > 0 }

func parseStream(body []byte) *streamInfo {
	s := &streamInfo{}
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if string(payload) == "[DONE]" {
			s.done = true
			continue
		}
		var chunk struct {
			Model   string          `json:"model"`
			Error   json.RawMessage `json:"error"`
			Usage   *usageJSON      `json:"usage"`
			Choices []struct {
				FinishReason *string `json:"finish_reason"`
				Delta        struct {
					Content      *string         `json:"content"`
					ToolCalls    json.RawMessage `json:"tool_calls"`
					FunctionCall json.RawMessage `json:"function_call"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(payload, &chunk); err != nil || (len(chunk.Error) > 0 && string(chunk.Error) != "null") {
			s.bad = true
			continue
		}
		if chunk.Model != "" {
			s.model = chunk.Model
		}
		if chunk.Usage != nil {
			s.promptTokens, s.completionTokens = chunk.Usage.PromptTokens, chunk.Usage.CompletionTokens
		}
		for _, c := range chunk.Choices {
			if c.Delta.Content != nil {
				s.content.WriteString(*c.Delta.Content)
			}
			if !empty(c.Delta.ToolCalls) || !empty(c.Delta.FunctionCall) {
				s.toolCalls = true
			}
			if c.FinishReason != nil {
				s.hasFinish, s.finish = true, *c.FinishReason
			}
		}
	}
	return s
}
