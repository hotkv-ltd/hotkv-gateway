// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowCache answers only after delay, giving up when the caller's context ends, like a real network call.
type slowCache struct {
	memCache
	delay time.Duration
}

func (c *slowCache) wait(ctx context.Context) error {
	select {
	case <-time.After(c.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *slowCache) Get(ctx context.Context, key string) (*Entry, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return c.memCache.Get(ctx, key)
}

func (c *slowCache) Set(ctx context.Context, key string, e *Entry, ttl time.Duration) error {
	if err := c.wait(ctx); err != nil {
		return err
	}
	return c.memCache.Set(ctx, key, e, ttl)
}

func newSlowCache(delay time.Duration) *slowCache {
	return &slowCache{memCache: memCache{m: map[string]*Entry{}}, delay: delay}
}

func TestPathsThatClimbOutAreRefused(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	r, _ := post(t, srv.URL, "/v1/../chat/completions", "Bearer k", chatBody)
	if r.StatusCode != http.StatusBadRequest || up.chat.Load() != 0 {
		t.Fatalf("status %d, %d upstream calls", r.StatusCode, up.chat.Load())
	}
}

func TestABodyWithTwoJSONObjectsIsNotCached(t *testing.T) {
	up := newUpstream(t)
	cache := newMemCache()
	srv := newGateway(t, up, cache, nil, nil)
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody+`{"model":"other"}`)
	if r.Header.Get(cacheHeader) != "BYPASS" || cache.size() != 0 {
		t.Fatalf("state %q, %d entries", r.Header.Get(cacheHeader), cache.size())
	}
}

func TestCallersWhoAuthenticateOnOtherHeadersAreKeptApart(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.TenantHeaders = []string{"X-Team-Token"} })
	for _, h := range []string{"X-Goog-Api-Key", "X-Team-Token"} {
		post(t, srv.URL, "/v1/chat/completions", "", chatBody, h, "alice")
		r, _ := post(t, srv.URL, "/v1/chat/completions", "", chatBody, h, "bob")
		if r.Header.Get(cacheHeader) != "MISS" {
			t.Fatalf("%s: bob was served alice's answer (%q)", h, r.Header.Get(cacheHeader))
		}
		r, _ = post(t, srv.URL, "/v1/chat/completions", "", chatBody, h, "alice")
		if r.Header.Get(cacheHeader) != "HIT" {
			t.Fatalf("%s: alice should hit her own entry, got %q", h, r.Header.Get(cacheHeader))
		}
	}
}

func TestAnUpstreamQueryIsKeptAndTheRequestQueryAdded(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.Upstream, _ = url.Parse(up.URL + "?api-version=7") })
	post(t, srv.URL, "/v1/chat/completions?x=2", "Bearer k", chatBody)
	if q, _ := up.lastQuery.Load().(string); q != "api-version=7&x=2" {
		t.Fatalf("upstream saw query %q", q)
	}
}

func TestOnlyTheLeaderOfCoalescedRequestsGetsUpstreamCookies(t *testing.T) {
	up := newUpstream(t)
	up.delay = 300 * time.Millisecond
	up.cookie.Store(true)
	srv := newGateway(t, up, newMemCache(), nil, nil)
	var wg sync.WaitGroup
	var cookies atomic.Int64
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
			if r.Header.Get("Set-Cookie") != "" {
				cookies.Add(1)
			}
		}()
	}
	wg.Wait()
	if up.chat.Load() != 1 || cookies.Load() != 1 {
		t.Fatalf("%d upstream calls, %d callers got the cookie", up.chat.Load(), cookies.Load())
	}
}

func TestMissesAndCoalescedRequestsAreCountedOncePerRequest(t *testing.T) {
	up := newUpstream(t)
	up.delay = 300 * time.Millisecond
	srv := newGateway(t, up, newMemCache(), nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
		}()
	}
	wg.Wait()
	m := metricsOf(srv)
	if m.Misses.Load() != 1 || m.Misses.Load()+m.Coalesced.Load() != 5 {
		t.Fatalf("misses %d, coalesced %d", m.Misses.Load(), m.Coalesced.Load())
	}
}

func TestTooManyRequestsInFlightGetA503(t *testing.T) {
	up := newUpstream(t)
	up.delay = 400 * time.Millisecond
	srv := newGateway(t, up, newMemCache(), nil, func(c *Config) { c.MaxInFlight = 1 })
	var wg sync.WaitGroup
	statuses := make([]int, 2)
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 80 * time.Millisecond)
			r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", fmt.Sprintf(`{"model":"m","messages":[{"role":"user","content":"q%d"}]}`, i))
			statuses[i] = r.StatusCode
		}(i)
	}
	wg.Wait()
	if statuses[0] != 200 || statuses[1] != http.StatusServiceUnavailable {
		t.Fatalf("statuses %v", statuses)
	}
}

func TestASlowCacheDoesNotSlowTheRequest(t *testing.T) {
	up := newUpstream(t)
	srv := newGateway(t, up, newSlowCache(5*time.Second), nil, func(c *Config) { c.CacheTimeout = 100 * time.Millisecond })
	start := time.Now()
	r, b := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r.StatusCode != 200 || !strings.Contains(b, "answer to:") {
		t.Fatalf("status %d body %q", r.StatusCode, b)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the request waited %v for the cache", took)
	}
	if metricsOf(srv).CacheErrors.Load() == 0 {
		t.Fatal("slow cache calls were not counted as errors")
	}
}

// streamUpstream runs fn as the handler of an event-stream upstream.
func streamUpstream(t *testing.T, fn func(w http.ResponseWriter, r *http.Request)) *upstream {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fn(w, r)
	}))
	t.Cleanup(s.Close)
	return &upstream{Server: s}
}

const firstChunk = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"par\"},\"finish_reason\":null}]}\n\n"

func TestAnUpstreamThatBreaksMidStreamAbortsTheCallerAndStoresNothing(t *testing.T) {
	up := streamUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, firstChunk)
		w.(http.Flusher).Flush()
		time.Sleep(50 * time.Millisecond)
		panic(http.ErrAbortHandler) // the connection drops
	})
	cache := newMemCache()
	srv := newGateway(t, up, cache, nil, nil)
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr == nil {
		t.Fatal("the caller saw a cleanly finished response although the stream broke")
	}
	drain(srv)
	if cache.size() != 0 || metricsOf(srv).UpstreamErrors.Load() != 1 {
		t.Fatalf("%d entries, %d upstream errors", cache.size(), metricsOf(srv).UpstreamErrors.Load())
	}
}

func TestACallerHangingUpCancelsTheUpstreamStream(t *testing.T) {
	cancelled := make(chan struct{})
	up := streamUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, firstChunk)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(cancelled)
	})
	srv := newGateway(t, up, newMemCache(), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	resp.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("the upstream call kept running after the caller left")
	}
}

func TestAStreamThatGoesQuietIsCut(t *testing.T) {
	up := streamUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, firstChunk)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	})
	cache := newMemCache()
	srv := newGateway(t, up, cache, nil, func(c *Config) { c.StreamIdleTimeout = 200 * time.Millisecond })
	start := time.Now()
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr == nil || time.Since(start) > 4*time.Second {
		t.Fatalf("read error %v after %v", readErr, time.Since(start))
	}
	if cache.size() != 0 {
		t.Fatal("a cut stream was stored")
	}
}

func TestAStreamBiggerThanTheEntryLimitIsRelayedButNotStored(t *testing.T) {
	up := streamUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 30; i++ {
			fmt.Fprintf(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":%q},\"finish_reason\":null}]}\n\n", strings.Repeat("y", 200))
		}
		fmt.Fprint(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	cache := newMemCache()
	srv := newGateway(t, up, cache, nil, func(c *Config) { c.MaxEntryBytes = 2048 })
	_, body := post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}]}`)
	drain(srv)
	if !strings.HasSuffix(body, "data: [DONE]\n\n") || strings.Count(body, "yyyy") < 30 {
		t.Fatalf("the caller did not get the whole stream (%d bytes)", len(body))
	}
	if cache.size() != 0 {
		t.Fatal("an oversize stream was stored")
	}
}

func TestSlowEmbeddingsPauseTheSemanticCacheInsteadOfHoldingRequestsUp(t *testing.T) {
	up := newUpstream(t)
	up.embDelay = 2 * time.Second
	sem := &memSemantic{threshold: 0.8}
	srv := newGateway(t, up, newMemCache(), sem, func(c *Config) { c.EmbeddingTimeout = 100 * time.Millisecond })
	ask := func(text string) time.Duration {
		start := time.Now()
		r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", fmt.Sprintf(`{"model":"gpt-test","messages":[{"role":"user","content":%q}]}`, text))
		if r.StatusCode != 200 || r.Header.Get(cacheHeader) != "MISS" {
			t.Fatalf("status %d state %q", r.StatusCode, r.Header.Get(cacheHeader))
		}
		return time.Since(start)
	}
	if took := ask("first question here"); took > 1500*time.Millisecond {
		t.Fatalf("the first request waited %v for embeddings", took)
	}
	calls := up.embeddings.Load()
	if took := ask("a second different question"); took > 500*time.Millisecond {
		t.Fatalf("the second request waited %v although the semantic cache is paused", took)
	}
	if up.embeddings.Load() != calls {
		t.Fatal("the embeddings endpoint was called again while the semantic cache was paused")
	}
	if len(sem.rows) != 0 {
		t.Fatal("something was stored without an embedding")
	}
}

func TestNoCacheStillTeachesTheSemanticCache(t *testing.T) {
	up := newUpstream(t)
	sem := &memSemantic{threshold: 0.8}
	srv := newGateway(t, up, newMemCache(), sem, nil)
	q := func(text string) string {
		return fmt.Sprintf(`{"model":"gpt-test","messages":[{"role":"user","content":%q}]}`, text)
	}
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my account password"), "X-Hotkv-Cache-Control", "no-cache")
	r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my password for the account"))
	if r.Header.Get(cacheHeader) != "SEMANTIC-HIT" {
		t.Fatalf("got %q", r.Header.Get(cacheHeader))
	}
}

func TestASemanticStreamHitHonoursIncludeUsage(t *testing.T) {
	up := newUpstream(t)
	sem := &memSemantic{threshold: 0.8}
	srv := newGateway(t, up, newMemCache(), sem, nil)
	post(t, srv.URL, "/v1/chat/completions", "Bearer k", `{"model":"gpt-test","messages":[{"role":"user","content":"how do I reset my account password"}]}`)
	_, plain := post(t, srv.URL, "/v1/chat/completions", "Bearer k",
		`{"model":"gpt-test","stream":true,"messages":[{"role":"user","content":"how do I reset my password for the account"}]}`)
	_, withUsage := post(t, srv.URL, "/v1/chat/completions", "Bearer k",
		`{"model":"gpt-test","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"how do I reset my password for the account"}]}`)
	if strings.Contains(plain, `"usage"`) || !strings.Contains(withUsage, `"usage"`) {
		t.Fatalf("usage chunk: without=%v with=%v", strings.Contains(plain, `"usage"`), strings.Contains(withUsage, `"usage"`))
	}
}

func TestAnErrorFromTheEmbeddingsEndpointNeverFailsTheChatRequest(t *testing.T) {
	up := newUpstream(t)
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/embeddings") {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"message":"no such model"}}`)
			return
		}
		up.Config.Handler.ServeHTTP(w, r)
	}))
	defer broken.Close()
	up2 := &upstream{Server: broken}
	srv := newGateway(t, up2, newMemCache(), &memSemantic{threshold: 0.8}, nil)
	r, b := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r.StatusCode != 200 || !strings.Contains(b, "answer to:") {
		t.Fatalf("status %d body %q", r.StatusCode, b)
	}
}
