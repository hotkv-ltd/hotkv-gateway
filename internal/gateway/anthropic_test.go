// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// anthUpstream imitates Anthropic's Messages API, in the wire format its documentation describes.
type anthUpstream struct {
	*upstream
	messages  atomic.Int64
	countCall atomic.Int64
	otherCall atomic.Int64
	lastBody  atomic.Value // the last request body of /v1/messages
	stop      atomic.Value // stop_reason to answer with ("" means end_turn)
	failure   atomic.Value // "event": send an error event mid-stream; "status": answer 529
	usage     anthropicUsage
}

func newAnthUpstream(t *testing.T) *anthUpstream {
	t.Helper()
	a := &anthUpstream{usage: anthropicUsage{InputTokens: 7, OutputTokens: 9, CacheCreationInputTokens: 50, CacheReadInputTokens: 100}}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/v1/messages":
			a.messages.Add(1)
			a.lastBody.Store(string(body))
			a.messagesHandler(w, body)
		case "/v1/messages/count_tokens":
			a.countCall.Add(1)
			fmt.Fprint(w, `{"input_tokens":5}`)
		default:
			a.otherCall.Add(1)
			fmt.Fprint(w, `{"ok":true}`)
		}
	}))
	t.Cleanup(s.Close)
	a.upstream = &upstream{Server: s}
	return a
}

func (a *anthUpstream) stopReason() string {
	if s, _ := a.stop.Load().(string); s != "" {
		return s
	}
	return "end_turn"
}

func (a *anthUpstream) messagesHandler(w http.ResponseWriter, body []byte) {
	var req struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &req) != nil || len(req.Messages) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"type":"error","error":{"type":"invalid_request_error","message":"bad request"}}`)
		return
	}
	failure, _ := a.failure.Load().(string)
	if failure == "status" {
		w.WriteHeader(529)
		fmt.Fprint(w, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
		return
	}
	text := "Reply to: " + req.Messages[len(req.Messages)-1].Content
	u := a.usage
	if !req.Stream {
		w.Header().Set("Content-Type", "application/json")
		out, _ := json.Marshal(map[string]any{
			"id": "msg_01", "type": "message", "role": "assistant", "model": req.Model,
			"content":     []any{map[string]any{"type": "text", "text": text}},
			"stop_reason": a.stopReason(), "stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens": u.InputTokens, "output_tokens": u.OutputTokens,
				"cache_creation_input_tokens": u.CacheCreationInputTokens, "cache_read_input_tokens": u.CacheReadInputTokens,
			},
		})
		w.Write(out)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fl := w.(http.Flusher)
	event := func(name string, data string) {
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, data)
		fl.Flush()
	}
	event("message_start", fmt.Sprintf(`{"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","content":[],"model":%q,"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":%d,"cache_creation_input_tokens":%d,"cache_read_input_tokens":%d,"output_tokens":1}}}`,
		req.Model, u.InputTokens, u.CacheCreationInputTokens, u.CacheReadInputTokens))
	event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	event("ping", `{"type":"ping"}`)
	half := len(text) / 2
	for _, part := range []string{text[:half], text[half:]} {
		event("content_block_delta", fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%q}}`, part))
	}
	event("content_block_stop", `{"type":"content_block_stop","index":0}`)
	if failure == "event" {
		event("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
		return
	}
	event("message_delta", fmt.Sprintf(`{"type":"message_delta","delta":{"stop_reason":%q,"stop_sequence":null},"usage":{"output_tokens":%d}}`, a.stopReason(), u.OutputTokens))
	event("message_stop", `{"type":"message_stop"}`)
}

const msgBody = `{"model":"claude-test","max_tokens":100,"messages":[{"role":"user","content":"What is HotKV?"}]}`

func anthPost(t *testing.T, srv *httptest.Server, path, key, body string, extra ...string) (*http.Response, string) {
	t.Helper()
	h := append([]string{"X-Api-Key", key, "Anthropic-Version", "2023-06-01"}, extra...)
	return post(t, srv.URL, path, "", body, h...)
}

func TestMessagesAreCachedAndHitsCountTheWholePrompt(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, nil)
	r1, b1 := anthPost(t, srv, "/v1/messages", "k1", msgBody)
	r2, b2 := anthPost(t, srv, "/v1/messages", "k1", msgBody)
	if r1.Header.Get(cacheHeader) != "MISS" || r2.Header.Get(cacheHeader) != "HIT" || b1 != b2 || up.messages.Load() != 1 {
		t.Fatalf("states %q %q, equal=%v, %d upstream calls", r1.Header.Get(cacheHeader), r2.Header.Get(cacheHeader), b1 == b2, up.messages.Load())
	}
	// a hit saves everything the provider billed for: 7 + 50 + 100 input and 9 output
	if got := metricsOf(srv).TokensSaved.Load(); got != 7+50+100+9 {
		t.Fatalf("tokens saved %d", got)
	}
	r3, _ := anthPost(t, srv, "/v1/messages", "someone-else", msgBody)
	if r3.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("another key must not see the answer, got %q", r3.Header.Get(cacheHeader))
	}
}

func TestMessageStreamsAreReplayedEventForEvent(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, nil)
	body := `{"model":"claude-test","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"stream it"}]}`
	r1, b1 := anthPost(t, srv, "/v1/messages", "k", body)
	drain(srv)
	r2, b2 := anthPost(t, srv, "/v1/messages", "k", body)
	if r1.Header.Get(cacheHeader) != "MISS" || r2.Header.Get(cacheHeader) != "HIT" || b1 != b2 || up.messages.Load() != 1 {
		t.Fatalf("states %q %q, equal=%v, %d upstream calls", r1.Header.Get(cacheHeader), r2.Header.Get(cacheHeader), b1 == b2, up.messages.Load())
	}
	for _, want := range []string{"event: message_start", "event: content_block_delta", "event: message_delta", "event: message_stop"} {
		if !strings.Contains(b2, want) {
			t.Fatalf("the replay lacks %q:\n%s", want, b2)
		}
	}
	if !strings.HasPrefix(r2.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content type %q", r2.Header.Get("Content-Type"))
	}
}

func TestMessagesThatAreNotFinishedAnswersAreNeverStored(t *testing.T) {
	up := newAnthUpstream(t)
	cache := newMemCache()
	srv := newGateway(t, up.upstream, cache, nil, nil)
	for _, reason := range []string{"pause_turn", "refusal"} {
		up.stop.Store(reason)
		anthPost(t, srv, "/v1/messages", "k", msgBody)
		anthPost(t, srv, "/v1/messages", "k", `{"model":"claude-test","stream":true,"messages":[{"role":"user","content":"x"}]}`)
		drain(srv)
	}
	if cache.size() != 0 {
		t.Fatalf("%d entries stored for answers that were not finished", cache.size())
	}
	up.stop.Store("")
	up.failure.Store("event")
	anthPost(t, srv, "/v1/messages", "k", `{"model":"claude-test","stream":true,"messages":[{"role":"user","content":"y"}]}`)
	drain(srv)
	if cache.size() != 0 {
		t.Fatal("a stream that ended in an error event was stored")
	}
	up.failure.Store("status")
	r, _ := anthPost(t, srv, "/v1/messages", "k", msgBody)
	if r.StatusCode != 529 || cache.size() != 0 {
		t.Fatalf("status %d, %d entries", r.StatusCode, cache.size())
	}
}

func TestBetaFeaturesAndVersionsAreKeptApartInTheCache(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, nil)
	anthPost(t, srv, "/v1/messages", "k", msgBody)
	r, _ := anthPost(t, srv, "/v1/messages", "k", msgBody, "Anthropic-Beta", "some-feature-2026-01-01")
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("a request with a beta header must not share an entry, got %q", r.Header.Get(cacheHeader))
	}
	r, _ = anthPost(t, srv, "/v1/messages", "k", msgBody, "Anthropic-Beta", "some-feature-2026-01-01")
	if r.Header.Get(cacheHeader) != "HIT" {
		t.Fatalf("the same beta header should hit, got %q", r.Header.Get(cacheHeader))
	}
	r, _ = post(t, srv.URL, "/v1/messages", "", msgBody, "X-Api-Key", "k", "Anthropic-Version", "2099-01-01")
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("another API version must not share an entry, got %q", r.Header.Get(cacheHeader))
	}
}

func TestOnlyTheMessagesEndpointIsCached(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, nil)
	for i := 0; i < 2; i++ {
		r, _ := anthPost(t, srv, "/v1/messages/count_tokens", "k", msgBody)
		if r.Header.Get(cacheHeader) != "" {
			t.Fatal("count_tokens went through the cache")
		}
		r, _ = anthPost(t, srv, "/v1/messages/batches", "k", `{"requests":[]}`)
		if r.Header.Get(cacheHeader) != "" {
			t.Fatal("batches went through the cache")
		}
		// OpenAI's Assistants API also has a POST .../messages: adding a message to a thread changes state
		r, _ = anthPost(t, srv, "/v1/threads/thread_1/messages", "k", `{"role":"user","content":"hi"}`)
		if r.Header.Get(cacheHeader) != "" {
			t.Fatal("a thread message went through the cache")
		}
	}
	if up.countCall.Load() != 2 || up.otherCall.Load() != 4 {
		t.Fatalf("count_tokens %d, other %d: every call must reach the upstream", up.countCall.Load(), up.otherCall.Load())
	}
}

func TestTheProvidersOwnPromptCacheIsVisibleInTheMetrics(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, nil)
	anthPost(t, srv, "/v1/messages", "k", msgBody)
	anthPost(t, srv, "/v1/messages", "k", msgBody) // a gateway hit: the provider is not asked, nothing is added
	m := metricsOf(srv)
	if m.UpstreamInput.Load() != 7 || m.UpstreamOutput.Load() != 9 || m.UpstreamCacheRead.Load() != 100 || m.UpstreamCacheWrite.Load() != 50 {
		t.Fatalf("input %d output %d read %d write %d", m.UpstreamInput.Load(), m.UpstreamOutput.Load(), m.UpstreamCacheRead.Load(), m.UpstreamCacheWrite.Load())
	}
	// a stream: input-side usage arrives in message_start, the output tokens in message_delta
	anthPost(t, srv, "/v1/messages", "k", `{"model":"claude-test","stream":true,"messages":[{"role":"user","content":"s"}]}`)
	drain(srv)
	if m.UpstreamCacheRead.Load() != 200 || m.UpstreamOutput.Load() != 18 {
		t.Fatalf("after a stream: read %d output %d", m.UpstreamCacheRead.Load(), m.UpstreamOutput.Load())
	}
	resp, _ := http.Get(srv.URL + "/_hotkv/metrics")
	text, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(text), `hotkv_gateway_upstream_tokens_total{type="cache_read"} 200`) {
		t.Fatalf("metrics:\n%s", text)
	}
}

func TestOpenAIPromptCacheUsageIsReadInBothShapes(t *testing.T) {
	cases := map[string]string{
		"chat":      `{"usage":{"prompt_tokens":40,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":30}}}`,
		"responses": `{"usage":{"input_tokens":40,"output_tokens":5,"input_tokens_details":{"cached_tokens":30}}}`,
	}
	for name, body := range cases {
		u, ok := usageOf(kindChat, []byte(body))
		if !ok || u != (upstreamUsage{input: 10, output: 5, cacheRead: 30}) {
			t.Errorf("%s: %+v %v", name, u, ok)
		}
	}
	u, _ := usageOf(kindChat, []byte(`{"usage":{"input_tokens":100,"output_tokens":1,"input_tokens_details":{"cached_tokens":60,"cache_write_tokens":25}}}`))
	if u != (upstreamUsage{input: 15, output: 1, cacheRead: 60, cacheWrite: 25}) || u.promptTotal() != 100 {
		t.Errorf("with cache writes: %+v", u)
	}
	if u, _ := usageOf(kindChat, []byte(`{"usage":{"prompt_tokens":3}}`)); u.promptTotal() != 3 {
		t.Errorf("a prompt with no cache details: %+v", u)
	}
}

func TestAutoCacheTurnsOnProviderCachingOnlyWhereItIsNotAlreadyUsed(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, func(c *Config) { c.AnthropicAutoCache = true })
	upstreamSaw := func() map[string]any {
		var m map[string]any
		json.Unmarshal([]byte(up.lastBody.Load().(string)), &m)
		return m
	}
	anthPost(t, srv, "/v1/messages", "k", msgBody)
	cc, _ := upstreamSaw()["cache_control"].(map[string]any)
	if cc["type"] != "ephemeral" || cc["ttl"] != nil {
		t.Fatalf("the upstream saw cache_control %v", cc)
	}
	// the cache key is made from what the caller sent, so the same request hits
	r, _ := anthPost(t, srv, "/v1/messages", "k", msgBody)
	if r.Header.Get(cacheHeader) != "HIT" {
		t.Fatalf("got %q", r.Header.Get(cacheHeader))
	}
	// a request that already places its own breakpoints is left exactly as it is
	own := `{"model":"claude-test","max_tokens":10,"system":[{"type":"text","text":"rules","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"own"}]}`
	anthPost(t, srv, "/v1/messages", "k", own)
	if _, top := upstreamSaw()["cache_control"]; top {
		t.Fatal("a top-level cache_control was added to a request that has its own breakpoints")
	}
	// other APIs are not touched
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
}

func TestAutoCacheCanAskForTheLongerLifetime(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, func(c *Config) { c.AnthropicAutoCache, c.AnthropicCacheTTL = true, "1h" })
	anthPost(t, srv, "/v1/messages", "k", msgBody)
	var m map[string]any
	json.Unmarshal([]byte(up.lastBody.Load().(string)), &m)
	if cc, _ := m["cache_control"].(map[string]any); cc["ttl"] != "1h" {
		t.Fatalf("cache_control %v", m["cache_control"])
	}
}

func TestAutoCacheIsOffByDefault(t *testing.T) {
	up := newAnthUpstream(t)
	srv := newGateway(t, up.upstream, newMemCache(), nil, nil)
	anthPost(t, srv, "/v1/messages", "k", msgBody)
	if strings.Contains(up.lastBody.Load().(string), "cache_control") {
		t.Fatal("the request was changed although the option is off")
	}
}

func TestWithAutoCacheKeepsTheRestOfTheRequest(t *testing.T) {
	in := `{"model":"m","seed":9007199254740993,"temperature":0.70,"messages":[{"role":"user","content":"<b>&</b>"}],"tools":[{"name":"t"}]}`
	out, did := withAutoCache([]byte(in), "5m")
	if !did {
		t.Fatal("not applied")
	}
	var back map[string]any
	dec := json.NewDecoder(strings.NewReader(string(out)))
	dec.UseNumber()
	if err := dec.Decode(&back); err != nil {
		t.Fatal(err)
	}
	if back["seed"].(json.Number).String() != "9007199254740993" || back["temperature"].(json.Number).String() != "0.70" {
		t.Fatalf("numbers changed: %v %v", back["seed"], back["temperature"])
	}
	msgs := back["messages"].([]any)[0].(map[string]any)
	if msgs["content"] != "<b>&</b>" || back["cache_control"] == nil {
		t.Fatalf("%v", back)
	}
	for _, unchanged := range []string{`not json`, `{"messages":[],"tools":[{"cache_control":{"type":"ephemeral"}}]}`, `[1,2]`} {
		if got, did := withAutoCache([]byte(unchanged), "5m"); did || string(got) != unchanged {
			t.Errorf("%s was changed", unchanged)
		}
	}
}

func TestAnthropicStreamUsageIsCumulativeAndStopReasonsAreChecked(t *testing.T) {
	s := parseAnthropicStream([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":4,\"output_tokens\":1}}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":12}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":15}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	if !s.complete() || s.model != "m" || s.usage != (upstreamUsage{input: 10, output: 15, cacheRead: 4}) {
		t.Fatalf("%+v complete=%v", s, s.complete())
	}
	if parseAnthropicStream([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n")).complete() {
		t.Fatal("a stream without message_stop was complete")
	}
	if parseAnthropicStream([]byte("data: not json\n")).complete() {
		t.Fatal("garbage was complete")
	}
}

func TestInspectMessages(t *testing.T) {
	info, ok := inspectMessages([]byte(`{"type":"message","model":"m","stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":4,"cache_read_input_tokens":5,"cache_creation_input_tokens":6}}`))
	if !ok || info.promptTokens != 14 || info.completionTokens != 4 {
		t.Fatalf("%+v %v", info, ok)
	}
	for name, body := range map[string]string{
		"error":     `{"type":"error","error":{"type":"overloaded_error"}}`,
		"no stop":   `{"type":"message","stop_reason":null}`,
		"refusal":   `{"type":"message","stop_reason":"refusal"}`,
		"pause":     `{"type":"message","stop_reason":"pause_turn"}`,
		"not a msg": `{"id":"x"}`,
		"junk":      `<html>`,
	} {
		if _, ok := inspectMessages([]byte(body)); ok {
			t.Errorf("%s was accepted", name)
		}
	}
	if kindOf("/v1/messages") != kindMessages || kindOf("/anthropic/v1/messages/") != kindMessages ||
		kindOf("/v1/messages/count_tokens") != kindNone || kindOf("/v1/threads/t/messages") != kindNone {
		t.Fatal("kindOf routes the wrong paths to the Messages cache")
	}
}
