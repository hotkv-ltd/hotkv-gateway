// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const maxSemanticPrompt = 8000

// semCtx carries one request through the semantic cache.
type semCtx struct {
	space  string // namespace: everything about the request except its final user message
	prompt string // that final user message
	tenant string
	vector []float32
}

// semanticContext says whether a request may use the semantic cache and, if so, where.
//
// Answers are matched by the meaning of the last user message only, so a hit is only safe
// when everything else that shapes the answer is identical. The namespace is therefore a
// digest of the whole request except that message (model, system prompt, parameters), and
// requests that carry earlier conversation turns, tools or functions never use it.
func (g *Gateway) semanticContext(r *http.Request, req map[string]any, tenant string) *semCtx {
	for _, field := range []string{"tools", "functions", "tool_choice", "function_call"} {
		if _, has := req[field]; has {
			return nil
		}
	}
	msgs, _ := req["messages"].([]any)
	if len(msgs) == 0 {
		return nil
	}
	last, _ := msgs[len(msgs)-1].(map[string]any)
	if role, _ := last["role"].(string); role != "user" {
		return nil
	}
	for _, m := range msgs[:len(msgs)-1] {
		mm, _ := m.(map[string]any)
		if role, _ := mm["role"].(string); role != "system" && role != "developer" {
			return nil
		}
	}
	prompt := messageText(last["content"])
	if prompt == "" || len(prompt) > maxSemanticPrompt {
		return nil
	}
	rest := make(map[string]any, len(req))
	for k, v := range req {
		if k != "messages" && k != "stream" && k != "stream_options" && !ignoredFields[k] {
			rest[k] = v
		}
	}
	rest["messages"] = msgs[:len(msgs)-1]
	var buf bytes.Buffer
	canonical(&buf, rest)
	sum := sha256.Sum256([]byte(keyVersion + "\n" + r.URL.Path + "\n" + r.URL.RawQuery + "\n" + tenant + "\n" + buf.String()))
	return &semCtx{space: g.cfg.Prefix + ":s:" + hex.EncodeToString(sum[:])[:24], prompt: prompt, tenant: tenant}
}

// messageText is the text of a message's content: a string, or the text parts of a list.
func messageText(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var parts []string
		for _, p := range c {
			pm, _ := p.(map[string]any)
			if t, _ := pm["type"].(string); t != "text" {
				return "" // an image or audio part: do not match on text alone
			}
			s, _ := pm["text"].(string)
			parts = append(parts, s)
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// reqInfo is what the semantic cache needs of a request, copied so it can be used after the handler returns.
type reqInfo struct {
	path, rawQuery string
	header         http.Header
}

func snapshot(r *http.Request) reqInfo {
	return reqInfo{path: r.URL.Path, rawQuery: r.URL.RawQuery, header: r.Header.Clone()}
}

// embeddingsDown is how long the semantic cache stays out of the way after the embeddings endpoint failed.
const embeddingsDown = 30 * time.Second

func (g *Gateway) embeddingsHealthy() bool {
	return time.Since(time.Unix(0, g.embedFailedAt.Load())) > embeddingsDown
}

func (g *Gateway) semanticLookup(ctx context.Context, ri reqInfo, sx *semCtx) (string, bool) {
	if !g.embeddingsHealthy() {
		return "", false
	}
	vec, err := g.embed(ctx, ri, sx)
	if err != nil {
		g.noteEmbedFailure(err)
		return "", false
	}
	sx.vector = vec
	lctx, cancel := context.WithTimeout(ctx, g.cfg.CacheTimeout)
	defer cancel()
	answer, _, ok, err := g.sem.Lookup(lctx, sx.space, vec)
	if err != nil {
		g.Metrics.CacheErrors.Add(1)
		g.log.Warn("semantic cache read failed", "error", err)
		return "", false
	}
	return answer, ok
}

func (g *Gateway) noteEmbedFailure(err error) {
	g.embedFailedAt.Store(time.Now().UnixNano())
	g.log.Warn("semantic cache paused: embeddings failed", "error", err, "for", embeddingsDown.String())
}

func (g *Gateway) semanticStore(ctx context.Context, ri reqInfo, sx *semCtx, answer string, usage *Entry) {
	if sx.vector == nil {
		if !g.embeddingsHealthy() {
			return
		}
		vec, err := g.embed(ctx, ri, sx)
		if err != nil {
			g.noteEmbedFailure(err)
			return
		}
		sx.vector = vec
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.CacheTimeout)
	defer cancel()
	if err := g.sem.Store(sctx, sx.space, sx.prompt, answer, sx.vector, g.cfg.TTL, usage); err != nil {
		g.Metrics.CacheErrors.Add(1)
		g.log.Warn("semantic cache write failed", "error", err)
	}
}

// embed turns the prompt into a vector with the upstream's embeddings endpoint, using the
// caller's own credentials. It has its own short time limit so a slow embeddings endpoint
// cannot hold up the request. The embedding itself goes through the exact cache, so asking
// the same question twice never pays for it twice.
func (g *Gateway) embed(ctx context.Context, ri reqInfo, sx *semCtx) ([]float32, error) {
	path := strings.TrimSuffix(strings.TrimSuffix(ri.path, "/"), "/chat/completions") + "/embeddings"
	req := map[string]any{"model": g.cfg.EmbeddingModel, "input": sx.prompt}
	key := requestKey(path, ri.rawQuery, sx.tenant, req)
	if e := g.lookup(ctx, key); e != nil {
		if vec, err := parseEmbedding([]byte(e.Body)); err == nil {
			return vec, nil
		}
	}
	body, _ := json.Marshal(req)
	header := http.Header{"Content-Type": {"application/json"}}
	for _, name := range tenantHeaderNames(g.cfg.TenantHeaders) {
		for _, v := range ri.header.Values(name) {
			header.Add(name, v)
		}
	}
	res := g.callWith(ctx, g.cfg.EmbeddingTimeout, http.MethodPost, path, ri.rawQuery, header, body)
	if res.err != nil {
		return nil, res.err
	}
	if res.status != http.StatusOK {
		return nil, fmt.Errorf("embeddings endpoint answered %d", res.status)
	}
	vec, err := parseEmbedding(res.body)
	if err != nil {
		return nil, err
	}
	if info, ok := inspectJSON(kindEmbeddings, res.body); ok && len(res.body) <= g.cfg.MaxEntryBytes {
		g.put(ctx, key, &Entry{
			Status: http.StatusOK, ContentType: "application/json", Body: string(res.body),
			Model: info.model, PromptTokens: info.promptTokens,
		})
	}
	return vec, nil
}

func parseEmbedding(body []byte) ([]float32, error) {
	var v struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}
	if len(v.Data) == 0 || len(v.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embeddings response has no vector")
	}
	return v.Data[0].Embedding, nil
}

// synthesize builds a chat completion (or its event stream) around a stored answer text.
func synthesize(req map[string]any, stream bool, answer string) *Entry {
	includeUsage := false
	if so, ok := req["stream_options"].(map[string]any); ok {
		includeUsage, _ = so["include_usage"].(bool)
	}
	model, _ := req["model"].(string)
	id := "chatcmpl-hotkv-" + randomHex(8)
	created := time.Now().Unix()
	if !stream {
		body, _ := json.Marshal(map[string]any{
			"id": id, "object": "chat.completion", "created": created, "model": model,
			"choices": []any{map[string]any{
				"index": 0, "message": map[string]any{"role": "assistant", "content": answer}, "finish_reason": "stop",
			}},
			"usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
		})
		return &Entry{Status: http.StatusOK, ContentType: "application/json", Body: string(body) + "\n", Model: model}
	}
	chunk := func(delta map[string]any, finish any) string {
		b, _ := json.Marshal(map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
		})
		return "data: " + string(b) + "\n\n"
	}
	var sb strings.Builder
	sb.WriteString(chunk(map[string]any{"role": "assistant", "content": ""}, nil))
	sb.WriteString(chunk(map[string]any{"content": answer}, nil))
	sb.WriteString(chunk(map[string]any{}, "stop"))
	if includeUsage {
		ub, _ := json.Marshal(map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model, "choices": []any{},
			"usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
		})
		sb.WriteString("data: " + string(ub) + "\n\n")
	}
	sb.WriteString("data: [DONE]\n\n")
	return &Entry{Status: http.StatusOK, ContentType: "text/event-stream", Stream: true, Body: sb.String(), Model: model}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
