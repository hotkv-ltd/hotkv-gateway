// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- fakes ----

type memCache struct {
	mu   sync.Mutex
	m    map[string]*Entry
	fail atomic.Bool
}

func newMemCache() *memCache { return &memCache{m: map[string]*Entry{}} }

func (c *memCache) Get(_ context.Context, key string) (*Entry, error) {
	if c.fail.Load() {
		return nil, errors.New("cache is down")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[key], nil
}

func (c *memCache) Set(_ context.Context, key string, e *Entry, _ time.Duration) error {
	if c.fail.Load() {
		return errors.New("cache is down")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *e
	c.m[key] = &cp
	return nil
}

func (c *memCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

type memSemantic struct {
	mu        sync.Mutex
	threshold float64
	rows      []semRow
}

type semRow struct {
	space  string
	vector []float32
	answer string
}

func (s *memSemantic) Lookup(_ context.Context, space string, v []float32) (string, float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	best, bestSim := "", -1.0
	for _, r := range s.rows {
		if r.space != space {
			continue
		}
		if sim := cosine(r.vector, v); sim > bestSim {
			best, bestSim = r.answer, sim
		}
	}
	return best, bestSim, bestSim >= s.threshold, nil
}

func (s *memSemantic) Store(_ context.Context, space, _ string, answer string, v []float32, _ time.Duration, _ *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, semRow{space, v, answer})
	return nil
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i] * b[i])
		na += float64(a[i] * a[i])
		nb += float64(b[i] * b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

var words = regexp.MustCompile(`[a-z]+`)

// fakeEmbedding: a bag of hashed words, so texts that share words are close.
func fakeEmbedding(text string) []float32 {
	v := make([]float32, 64)
	for _, w := range words.FindAllString(strings.ToLower(text), -1) {
		h := 0
		for _, c := range w {
			h = h*31 + int(c)
		}
		v[(h%64+64)%64]++
	}
	return v
}

type upstream struct {
	*httptest.Server
	chat       atomic.Int64
	embeddings atomic.Int64
	other      atomic.Int64
	delay      time.Duration
	embDelay   time.Duration
	cookie     atomic.Bool
	status     atomic.Int64 // 0 means 200
	lastAuth   atomic.Value
	lastQuery  atomic.Value
	reply      func(prompt string) string
}

func newUpstream(t *testing.T) *upstream {
	t.Helper()
	u := &upstream{reply: func(p string) string { return "answer to: " + p }}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.lastAuth.Store(r.Header.Get("Authorization"))
		u.lastQuery.Store(r.URL.RawQuery)
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.HasSuffix(r.URL.Path, "/chat/completions"):
			u.chat.Add(1)
			time.Sleep(u.delay)
			if u.cookie.Load() {
				http.SetCookie(w, &http.Cookie{Name: "session", Value: "secret"})
			}
			if st := int(u.status.Load()); st != 0 {
				w.WriteHeader(st)
				fmt.Fprint(w, `{"error":{"message":"boom"}}`)
				return
			}
			var req struct {
				Model    string `json:"model"`
				Stream   bool   `json:"stream"`
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			if json.Unmarshal(body, &req) != nil || len(req.Messages) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, `{"error":{"message":"invalid request"}}`)
				return
			}
			text := u.reply(req.Messages[len(req.Messages)-1].Content)
			if req.Stream {
				w.Header().Set("Content-Type", "text/event-stream")
				fl := w.(http.Flusher)
				for i, part := range []string{text[:len(text)/2], text[len(text)/2:]} {
					role := ""
					if i == 0 {
						role = `"role":"assistant",`
					}
					fmt.Fprintf(w, "data: {\"id\":\"c1\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{%s\"content\":%q},\"finish_reason\":null}]}\n\n", req.Model, role, part)
					fl.Flush()
				}
				fmt.Fprintf(w, "data: {\"id\":\"c1\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", req.Model)
				fmt.Fprint(w, "data: [DONE]\n\n")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"c1","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`, req.Model, text)
		case strings.HasSuffix(r.URL.Path, "/embeddings"):
			u.embeddings.Add(1)
			time.Sleep(u.embDelay)
			var req struct {
				Input string `json:"input"`
			}
			_ = json.Unmarshal(body, &req)
			vec, _ := json.Marshal(fakeEmbedding(req.Input))
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":%s}],"model":"emb","usage":{"prompt_tokens":3,"total_tokens":3}}`, vec)
		default:
			u.other.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"object":"list","data":[{"id":"gpt-test"}]}`)
		}
	}))
	t.Cleanup(u.Close)
	return u
}

func newGateway(t *testing.T, up *upstream, cache Cache, sem Semantic, mutate func(*Config)) *httptest.Server {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Upstream, _ = url.Parse(up.URL)
	cfg.Semantic = sem != nil
	cfg.SemanticThreshold = 0.8
	if mutate != nil {
		mutate(&cfg)
	}
	gw, err := New(cfg, cache, sem, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gw)
	t.Cleanup(srv.Close)
	srv.Config.Handler = gw
	gwByURL.Store(srv.URL, gw)
	return srv
}

var gwByURL sync.Map

func drain(srv *httptest.Server) {
	g, _ := gwByURL.Load(srv.URL)
	g.(*Gateway).Drain()
}

func metricsOf(srv *httptest.Server) *Metrics {
	g, _ := gwByURL.Load(srv.URL)
	return &g.(*Gateway).Metrics
}

func post(t *testing.T, base, path, auth string, body string, header ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

const chatBody = `{"model":"gpt-test","messages":[{"role":"user","content":"What is HotKV?"}]}`

// ---- tests ----

func TestSecondIdenticalRequestIsServedFromTheCache(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	r1, b1 := post(t, srv.URL, "/v1/chat/completions", "Bearer k1", chatBody)
	r2, b2 := post(t, srv.URL, "/v1/chat/completions", "Bearer k1", chatBody)
	if r1.Header.Get(cacheHeader) != "MISS" || r2.Header.Get(cacheHeader) != "HIT" {
		t.Fatalf("states %q then %q", r1.Header.Get(cacheHeader), r2.Header.Get(cacheHeader))
	}
	if b1 != b2 || up.chat.Load() != 1 {
		t.Fatalf("bodies equal=%v upstream calls=%d", b1 == b2, up.chat.Load())
	}
	if got := metricsOf(srv).TokensSaved.Load(); got != 15 {
		t.Fatalf("tokens saved = %d, want 15", got)
	}
	if r2.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("content type %q", r2.Header.Get("Content-Type"))
	}
}

func TestRequestsThatMeanTheSameShareAnEntry(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"gpt-test","temperature":0.70,"user":"alice","messages":[{"role":"user","content":"hi"}]}`)
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"messages":[{"content":"hi","role":"user"}],"user":"bob","temperature":0.7,"model":"gpt-test"}`)
	if r.Header.Get(cacheHeader) != "HIT" {
		t.Fatalf("expected a hit, got %q", r.Header.Get(cacheHeader))
	}
	r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"gpt-test","temperature":0.2,"messages":[{"role":"user","content":"hi"}]}`)
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("a different temperature must not share the entry, got %q", r.Header.Get(cacheHeader))
	}
}

func TestCallersDoNotShareAnswers(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	post(t, srv.URL, "/v1/chat/completions", "Bearer alice", chatBody)
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer bob", chatBody)
	if r.Header.Get(cacheHeader) != "MISS" || up.chat.Load() != 2 {
		t.Fatalf("bob got %q after %d upstream calls", r.Header.Get(cacheHeader), up.chat.Load())
	}
	global := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.Scope = ScopeGlobal })
	post(t, global.URL, "/v1/chat/completions", "Bearer alice", chatBody)
	r, _ = post(t, global.URL, "/v1/chat/completions", "Bearer bob", chatBody)
	if r.Header.Get(cacheHeader) != "HIT" {
		t.Fatalf("global scope should share, got %q", r.Header.Get(cacheHeader))
	}
}

func TestStreamsAreCapturedAndReplayed(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	body := `{"model":"gpt-test","stream":true,"messages":[{"role":"user","content":"stream me"}]}`
	r1, b1 := post(t, srv.URL, "/v1/chat/completions", "Bearer k", body)
	drain(srv)
	r2, b2 := post(t, srv.URL, "/v1/chat/completions", "Bearer k", body)
	if r1.Header.Get(cacheHeader) != "MISS" || r2.Header.Get(cacheHeader) != "HIT" {
		t.Fatalf("states %q then %q", r1.Header.Get(cacheHeader), r2.Header.Get(cacheHeader))
	}
	if b1 != b2 || !strings.HasSuffix(b2, "data: [DONE]\n\n") || up.chat.Load() != 1 {
		t.Fatalf("replay differs or upstream called %d times:\n%q\n%q", up.chat.Load(), b1, b2)
	}
	if !strings.HasPrefix(r2.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content type %q", r2.Header.Get("Content-Type"))
	}
	// a non-streaming request for the same prompt is a different entry
	r3, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"gpt-test","messages":[{"role":"user","content":"stream me"}]}`)
	if r3.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("got %q", r3.Header.Get(cacheHeader))
	}
}

func TestAnInterruptedStreamIsNotCached(t *testing.T) {
	cut := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"par\"},\"finish_reason\":null}]}\n\n")
	}))
	defer cut.Close()
	up := &upstream{Server: cut}
	cache := newCache()
	srv := newGateway(t, up, cache, nil, nil)
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}]}`)
	if cache.size() != 0 {
		t.Fatal("a stream without [DONE] must not be stored")
	}
}

func newCache() *memCache { return newMemCache() }

func TestErrorsAreNeverCached(t *testing.T) {
	up := newUpstream(t)
	cache := newMemCache()
	srv := newGateway(t, up, cache, nil, nil)
	up.status.Store(500)
	r, b := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r.StatusCode != 500 || !strings.Contains(b, "boom") {
		t.Fatalf("status %d body %q", r.StatusCode, b)
	}
	if cache.size() != 0 {
		t.Fatal("an error response was cached")
	}
	up.status.Store(0)
	r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r.StatusCode != 200 || r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("status %d state %q", r.StatusCode, r.Header.Get(cacheHeader))
	}
}

func TestCachePolicyHeadersAndRules(t *testing.T) {
	up := newUpstream(t)
	cache := newMemCache()
	srv := newGateway(t, up, cache, nil, func(c *Config) { c.MaxTemperature = 0.5 })

	post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody, "X-Hotkv-Cache-Control", "no-cache")
	if r.Header.Get(cacheHeader) != "MISS" || up.chat.Load() != 2 {
		t.Fatalf("no-cache should skip the lookup: %q, %d calls", r.Header.Get(cacheHeader), up.chat.Load())
	}
	r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody, "Cache-Control", "bypass")
	if r.Header.Get(cacheHeader) != "BYPASS" {
		t.Fatalf("got %q", r.Header.Get(cacheHeader))
	}
	other := `{"model":"gpt-test","messages":[{"role":"user","content":"other"}]}`
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", other, "X-Hotkv-Cache-Control", "no-store")
	r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", other)
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("no-store must not keep the answer, got %q", r.Header.Get(cacheHeader))
	}
	for _, body := range []string{
		`{"model":"gpt-test","n":2,"messages":[{"role":"user","content":"a"}]}`,
		`{"model":"gpt-test","temperature":0.9,"messages":[{"role":"user","content":"a"}]}`,
		`not json`,
	} {
		r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", body)
		if r.Header.Get(cacheHeader) != "BYPASS" {
			t.Fatalf("%s: got %q", body, r.Header.Get(cacheHeader))
		}
	}
}

func TestOnlyChosenModelsAreCached(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.CachedModels = []string{"gpt-test"} })
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"other","messages":[{"role":"user","content":"a"}]}`)
	if r.Header.Get(cacheHeader) != "BYPASS" {
		t.Fatalf("got %q", r.Header.Get(cacheHeader))
	}
}

func TestIdenticalRequestsInFlightShareOneUpstreamCall(t *testing.T) {
	up := newUpstream(t)
	up.delay = 300 * time.Millisecond
	srv := newGateway(t, up, newMemCache(), nil, nil)
	var wg sync.WaitGroup
	states := make([]string, 8)
	for i := range states {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
			states[i] = r.Header.Get(cacheHeader)
		}(i)
	}
	wg.Wait()
	if up.chat.Load() != 1 {
		t.Fatalf("upstream called %d times, states %v", up.chat.Load(), states)
	}
	if metricsOf(srv).Coalesced.Load() == 0 {
		t.Fatalf("no request was counted as coalesced: %v", states)
	}
}

func TestAnUnavailableCacheNeverFailsARequest(t *testing.T) {
	up := newUpstream(t)
	cache := newMemCache()
	cache.fail.Store(true)
	srv := newGateway(t, up, cache, nil, nil)
	r, b := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r.StatusCode != 200 || !strings.Contains(b, "answer to: What is HotKV?") {
		t.Fatalf("status %d body %q", r.StatusCode, b)
	}
	if metricsOf(srv).CacheErrors.Load() == 0 {
		t.Fatal("cache errors were not counted")
	}
}

func TestOtherEndpointsAreProxiedUntouched(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer caller-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), "gpt-test") || up.other.Load() != 1 {
		t.Fatalf("status %d body %q", resp.StatusCode, b)
	}
	if up.lastAuth.Load() != "Bearer caller-key" {
		t.Fatalf("upstream saw %v", up.lastAuth.Load())
	}
}

func TestTheGatewayCanHoldTheUpstreamKey(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.UpstreamAPIKey = "sk-held" })
	post(t, srv.URL, "/v1/chat/completions", "Bearer whatever", chatBody)
	if up.lastAuth.Load() != "Bearer sk-held" {
		t.Fatalf("upstream saw %v", up.lastAuth.Load())
	}
}

func TestEmbeddingsAreCached(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	body := `{"model":"emb","input":"hello"}`
	post(t, srv.URL, "/v1/embeddings", "Bearer k", body)
	r, _ := post(t, srv.URL, "/v1/embeddings", "Bearer k", body)
	if r.Header.Get(cacheHeader) != "HIT" || up.embeddings.Load() != 1 {
		t.Fatalf("state %q, %d upstream calls", r.Header.Get(cacheHeader), up.embeddings.Load())
	}
}

func TestSemanticCacheAnswersARephrasedQuestion(t *testing.T) {
	up := newUpstream(t)
	sem := &memSemantic{threshold: 0.8}
	srv := newGateway(t, up, newMemCache(), sem, nil)
	q := func(text string, stream bool) string {
		return fmt.Sprintf(`{"model":"gpt-test","stream":%v,"messages":[{"role":"system","content":"Be brief."},{"role":"user","content":%q}]}`, stream, text)
	}
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my account password", false))
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("got %q", r.Header.Get(cacheHeader))
	}
	r, b := post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my password for the account", false))
	if r.Header.Get(cacheHeader) != "SEMANTIC-HIT" || up.chat.Load() != 1 {
		t.Fatalf("state %q, %d chat calls", r.Header.Get(cacheHeader), up.chat.Load())
	}
	var parsed struct {
		Choices []struct {
			Message struct{ Content string } `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(b), &parsed); err != nil || !strings.HasPrefix(parsed.Choices[0].Message.Content, "answer to: how do I reset my account password") {
		t.Fatalf("body %q (%v)", b, err)
	}
	// the same hit can be asked for as a stream
	r, b = post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my password for the account", true))
	if r.Header.Get(cacheHeader) != "SEMANTIC-HIT" || !strings.Contains(b, "data: [DONE]") || !strings.Contains(b, `"finish_reason":"stop"`) {
		t.Fatalf("state %q body %q", r.Header.Get(cacheHeader), b)
	}
	// an unrelated question is not answered from the cache
	r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("what is the capital of france", false))
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("got %q", r.Header.Get(cacheHeader))
	}
	// a different system prompt is a different namespace
	other := `{"model":"gpt-test","messages":[{"role":"system","content":"Answer in French."},{"role":"user","content":"how do I reset my account password"}]}`
	r, _ = post(t, srv.URL, "/v1/chat/completions", "Bearer k", other)
	if r.Header.Get(cacheHeader) != "MISS" {
		t.Fatalf("a different system prompt must not match, got %q", r.Header.Get(cacheHeader))
	}
}

func TestSemanticCacheIsSkippedForConversationsAndTools(t *testing.T) {
	up := newUpstream(t)
	sem := &memSemantic{threshold: 0.5}
	srv := newGateway(t, up, newMemCache(), sem, nil)
	multi := `{"model":"gpt-test","messages":[{"role":"user","content":"hi there"},{"role":"assistant","content":"hello"},{"role":"user","content":"how do I reset my password"}]}`
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", multi)
	tools := `{"model":"gpt-test","tools":[{"type":"function","function":{"name":"f"}}],"messages":[{"role":"user","content":"how do I reset my password"}]}`
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", tools)
	if len(sem.rows) != 0 {
		t.Fatalf("%d rows stored for requests that must not use the semantic cache", len(sem.rows))
	}
}

func TestHealthAndMetrics(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	resp, _ := http.Get(srv.URL + "/_hotkv/health")
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), `"status":"ok"`) {
		t.Fatalf("health: %s", b)
	}
	resp, _ = http.Get(srv.URL + "/_hotkv/metrics")
	b, _ = io.ReadAll(resp.Body)
	for _, want := range []string{`hotkv_gateway_requests_total{result="hit"} 1`, `hotkv_gateway_requests_total{result="miss"} 1`, "hotkv_gateway_tokens_saved_total 15"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("metrics missing %q:\n%s", want, b)
		}
	}
}

func TestUnreachableUpstreamIsABadGateway(t *testing.T) {
	up := newUpstream(t)
	up.Close()
	srv := newGateway(t, up, newMemCache(), nil, nil)
	r, b := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r.StatusCode != http.StatusBadGateway || !strings.Contains(b, "gateway_error") {
		t.Fatalf("status %d body %q", r.StatusCode, b)
	}
}

func TestOversizedRequestsAreRefused(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.MaxRequestBytes = 2048 })
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"m","messages":[{"role":"user","content":"`+strings.Repeat("x", 4000)+`"}]}`)
	if r.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", r.StatusCode)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err == nil {
		t.Fatal("a config without an upstream must be refused")
	}
	cfg.Upstream, _ = url.Parse("https://api.openai.com")
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Scope = "everyone"
	if cfg.Validate() == nil {
		t.Fatal("unknown scope accepted")
	}
}
