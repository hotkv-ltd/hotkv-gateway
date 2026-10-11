// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
)

// upstreamUsage is what the provider reported for one answer, in additive categories:
// the input tokens split into those read from the provider's own prompt cache, those written
// to it, and the rest, plus the output tokens. input+cacheRead+cacheWrite is the whole prompt.
type upstreamUsage struct {
	input, output, cacheRead, cacheWrite uint64
}

func (u upstreamUsage) promptTotal() uint64 { return u.input + u.cacheRead + u.cacheWrite }

func (u upstreamUsage) isZero() bool { return u == upstreamUsage{} }

// jsonInfo is what the gateway needs to know about an upstream answer.
type jsonInfo struct {
	model            string
	promptTokens     uint64 // the whole prompt, which a cache hit saves
	completionTokens uint64
	usage            upstreamUsage
	plainText        bool   // one plain-text answer (no tool calls), which the semantic cache may keep
	text             string // that answer
}

// openAIUsage covers both shapes OpenAI-compatible APIs use for usage.
type openAIUsage struct {
	PromptTokens     uint64 `json:"prompt_tokens"`
	CompletionTokens uint64 `json:"completion_tokens"`
	InputTokens      uint64 `json:"input_tokens"`
	OutputTokens     uint64 `json:"output_tokens"`
	PromptDetails    struct {
		Cached     uint64 `json:"cached_tokens"`
		CacheWrite uint64 `json:"cache_write_tokens"`
	} `json:"prompt_tokens_details"`
	InputDetails struct {
		Cached     uint64 `json:"cached_tokens"`
		CacheWrite uint64 `json:"cache_write_tokens"`
	} `json:"input_tokens_details"`
}

func (u openAIUsage) normalise() upstreamUsage {
	in, out := u.PromptTokens, u.CompletionTokens
	if in == 0 {
		in = u.InputTokens
	}
	if out == 0 {
		out = u.OutputTokens
	}
	cached, write := u.PromptDetails.Cached, u.PromptDetails.CacheWrite
	if cached == 0 && write == 0 {
		cached, write = u.InputDetails.Cached, u.InputDetails.CacheWrite
	}
	rest := uint64(0)
	if in > cached+write {
		rest = in - cached - write
	}
	return upstreamUsage{input: rest, output: out, cacheRead: cached, cacheWrite: write}
}

func infoFromUsage(model string, u upstreamUsage) jsonInfo {
	return jsonInfo{model: model, promptTokens: u.promptTotal(), completionTokens: u.output, usage: u}
}

// inspectJSON decides whether a non-streaming answer is worth keeping.
// Errors, answers cut by a filter and anything unexpected are not.
func inspectJSON(k kind, body []byte) (jsonInfo, bool) {
	if k == kindMessages {
		return inspectMessages(body)
	}
	var v struct {
		Model   string          `json:"model"`
		Error   json.RawMessage `json:"error"`
		Usage   openAIUsage     `json:"usage"`
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
	info := infoFromUsage(v.Model, v.Usage.normalise())
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
		finished := *v.Choices[0].FinishReason == "stop" // a cut-short answer is not reused for other wordings
		if finished && empty(m.ToolCalls) && empty(m.FunctionCall) && json.Unmarshal(m.Content, &text) == nil && text != "" {
			info.plainText, info.text = true, text
		}
	}
	return info, true
}

// usageOf reads the usage of an answer, whether or not the answer may be cached.
func usageOf(k kind, body []byte) (upstreamUsage, bool) {
	if k == kindMessages {
		var v struct {
			Usage anthropicUsage `json:"usage"`
		}
		if json.Unmarshal(body, &v) != nil {
			return upstreamUsage{}, false
		}
		return v.Usage.normalise(), true
	}
	var v struct {
		Usage openAIUsage `json:"usage"`
	}
	if json.Unmarshal(body, &v) != nil {
		return upstreamUsage{}, false
	}
	return v.Usage.normalise(), true
}

func empty(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null" || s == "[]"
}

// streamInfo is what a captured server-sent event stream of an OpenAI-style API contains.
type streamInfo struct {
	done             bool // saw data: [DONE]
	bad              bool // a line that is not a normal chunk, such as an error
	model            string
	finish           string
	hasFinish        bool
	toolCalls        bool
	content          strings.Builder
	usage            upstreamUsage
	promptTokens     uint64
	completionTokens uint64
}

func (s *streamInfo) finished() bool {
	return !s.bad && s.hasFinish && s.finish != "content_filter"
}

func (s *streamInfo) plainText() bool {
	return !s.toolCalls && s.content.Len() > 0 && s.finish == "stop"
}

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
			Usage   *openAIUsage    `json:"usage"`
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
			s.usage = chunk.Usage.normalise()
			s.promptTokens, s.completionTokens = s.usage.promptTotal(), s.usage.output
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

// streamResult is the part of a captured stream the relay needs, whichever API produced it.
type streamResult struct {
	complete  bool // the stream ended normally and may be kept
	model     string
	usage     upstreamUsage
	plainText bool
	text      string
}

func parseStreamFor(k kind, body []byte) streamResult {
	if k == kindMessages {
		s := parseAnthropicStream(body)
		return streamResult{complete: s.complete(), model: s.model, usage: s.usage}
	}
	s := parseStream(body)
	return streamResult{
		complete: s.done && s.finished(), model: s.model, usage: s.usage,
		plainText: s.plainText(), text: s.content.String(),
	}
}
