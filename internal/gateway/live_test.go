// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests need a HotKV server: HOTKV_TEST_URL=hotkv://127.0.0.1:6379 go test ./...
// They use a random key prefix and remove what they create. They run against a
// licensed server (prompt cache and semantic cache) and an unlicensed one (plain keys).

func liveCache(t *testing.T) (*HotKVCache, string) {
	t.Helper()
	url := os.Getenv("HOTKV_TEST_URL")
	if url == "" {
		t.Skip("set HOTKV_TEST_URL to run the live tests")
	}
	prefix := fmt.Sprintf("gwtest-%d", time.Now().UnixNano())
	c, err := NewHotKVCache(context.Background(), url, prefix, time.Minute, 50, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		c.client.PCacheDrop(ctx, prefix+":exact")
		if spaces, err := c.client.SCacheList(ctx).Result(); err == nil {
			for _, ns := range spaces {
				if strings.HasPrefix(ns, prefix+":") {
					c.client.SCacheDrop(ctx, ns)
				}
			}
		}
		keys, _ := c.client.Keys(ctx, prefix+":*").Result()
		if len(keys) > 0 {
			c.client.Del(ctx, keys...)
		}
		c.Close()
	})
	mode := "prompt-cache"
	if c.Plain() {
		mode = "plain"
	}
	t.Logf("server mode: %s", mode)
	return c, mode
}

func TestLiveEntryRoundTrip(t *testing.T) {
	c, _ := liveCache(t)
	ctx := context.Background()
	if e, err := c.Get(ctx, "missing"); e != nil || err != nil {
		t.Fatalf("miss returned %v %v", e, err)
	}
	want := &Entry{Status: 200, ContentType: "application/json", Body: `{"a":"é \" \n 日本"}`, Model: "m", PromptTokens: 7, CompletionTokens: 3}
	if err := c.Set(ctx, "k1", want, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "k1")
	if err != nil || got == nil || *got != *want {
		t.Fatalf("got %+v, %v", got, err)
	}
	stream := &Entry{Status: 200, ContentType: "text/event-stream", Stream: true, Body: "data: {\"x\":1}\n\ndata: [DONE]\n\n"}
	if err := c.Set(ctx, "k2", stream, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Get(ctx, "k2"); got == nil || got.Body != stream.Body || !got.Stream {
		t.Fatalf("stream entry came back as %+v", got)
	}
}

func TestLiveEntriesExpire(t *testing.T) {
	c, _ := liveCache(t)
	ctx := context.Background()
	if err := c.Set(ctx, "short", &Entry{Status: 200, Body: "x"}, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3500 * time.Millisecond) // the server keeps expiry in whole seconds, so a 1 s entry can live up to 2 s
	if e, _ := c.Get(ctx, "short"); e != nil {
		t.Fatal("entry outlived its ttl")
	}
}

func TestLiveSemanticLookup(t *testing.T) {
	c, mode := liveCache(t)
	ctx := context.Background()
	if err := c.SemanticAvailable(ctx); err != nil {
		if mode == "plain" {
			t.Logf("unlicensed server refuses the semantic cache, as expected: %v", err)
			return
		}
		t.Fatal(err)
	}
	space := c.prefix + ":s:test"
	a, b := fakeEmbedding("reset my account password"), fakeEmbedding("completely different words here")
	if err := c.Store(ctx, space, "reset my account password", "use the link", a, time.Minute, &Entry{Model: "m", PromptTokens: 5, CompletionTokens: 5}); err != nil {
		t.Fatal(err)
	}
	ans, sim, ok, err := c.Lookup(ctx, space, fakeEmbedding("reset account password my"))
	if err != nil || !ok || ans != "use the link" || sim < 0.9 {
		t.Fatalf("lookup: %q %.3f %v %v", ans, sim, ok, err)
	}
	if _, _, ok, _ := c.Lookup(ctx, space, b); ok {
		t.Fatal("an unrelated prompt matched")
	}
}

func TestLiveGatewayEndToEnd(t *testing.T) {
	c, mode := liveCache(t)
	up := newUpstream(t)
	var sem Semantic
	if c.SemanticAvailable(context.Background()) == nil {
		sem = c
	}
	// the gateway's prefix is the test's own, so the semantic namespaces it creates are cleaned up with the rest
	srv := newGateway(t, up, c, sem, func(cfg *Config) { cfg.CacheTimeout = 2 * time.Second; cfg.Prefix = c.prefix })
	r1, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	r2, b2 := post(t, srv.URL, "/v1/chat/completions", "Bearer k", chatBody)
	if r1.Header.Get(cacheHeader) != "MISS" || r2.Header.Get(cacheHeader) != "HIT" || up.chat.Load() != 1 {
		t.Fatalf("[%s] %q then %q, %d upstream calls", mode, r1.Header.Get(cacheHeader), r2.Header.Get(cacheHeader), up.chat.Load())
	}
	if !strings.Contains(b2, "answer to: What is HotKV?") {
		t.Fatalf("body %q", b2)
	}
	stream := `{"model":"gpt-test","stream":true,"messages":[{"role":"user","content":"stream it"}]}`
	_, s1 := post(t, srv.URL, "/v1/chat/completions", "Bearer k", stream)
	drain(srv)
	r4, s2 := post(t, srv.URL, "/v1/chat/completions", "Bearer k", stream)
	if r4.Header.Get(cacheHeader) != "HIT" || s1 != s2 {
		t.Fatalf("stream replay: %q equal=%v", r4.Header.Get(cacheHeader), s1 == s2)
	}
	if sem != nil {
		q := func(text string) string {
			return fmt.Sprintf(`{"model":"gpt-test","messages":[{"role":"user","content":%q}]}`, text)
		}
		post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my account password"))
		r, _ := post(t, srv.URL, "/v1/chat/completions", "Bearer k", q("how do I reset my password for the account"))
		if r.Header.Get(cacheHeader) != "SEMANTIC-HIT" {
			t.Fatalf("semantic: %q", r.Header.Get(cacheHeader))
		}
	}
}

func TestLiveSemanticLookupNeverCreatesANamespace(t *testing.T) {
	c, mode := liveCache(t)
	ctx := context.Background()
	if mode == "plain" || c.SemanticAvailable(ctx) != nil {
		t.Skip("needs a server licensed for the semantic cache")
	}
	space := c.prefix + ":s:never"
	_, _, ok, err := c.Lookup(ctx, space, fakeEmbedding("anything at all"))
	if err != nil || ok {
		t.Fatalf("lookup in a namespace that does not exist: ok=%v err=%v", ok, err)
	}
	spaces, _ := c.client.SCacheList(ctx).Result()
	for _, ns := range spaces {
		if ns == space {
			t.Fatal("a lookup created a namespace")
		}
	}
	if err := c.Store(ctx, space, "q", "a", fakeEmbedding("anything at all"), time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := c.Lookup(ctx, space, fakeEmbedding("anything at all")); !ok {
		t.Fatal("the stored answer was not found")
	}
}
