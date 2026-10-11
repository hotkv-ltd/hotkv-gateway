// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	hotkv "github.com/hotkv-ltd/hotkv-go-client"
)

// Entry is a cached response.
type Entry struct {
	Status           int    `json:"s"`
	ContentType      string `json:"t"`
	Stream           bool   `json:"x,omitempty"` // Body is a server-sent event stream
	Body             string `json:"b"`
	Model            string `json:"m,omitempty"`
	PromptTokens     uint64 `json:"p,omitempty"`
	CompletionTokens uint64 `json:"c,omitempty"`
}

// Cache stores answers by exact request key.
type Cache interface {
	// Get returns nil, nil on a miss.
	Get(ctx context.Context, key string) (*Entry, error)
	Set(ctx context.Context, key string, e *Entry, ttl time.Duration) error
}

// Semantic stores answers by the meaning of the prompt.
type Semantic interface {
	// Lookup returns the answer text stored for the nearest prompt, or ok=false.
	Lookup(ctx context.Context, space string, vector []float32) (answer string, similarity float64, ok bool, err error)
	Store(ctx context.Context, space, prompt, answer string, vector []float32, ttl time.Duration, usage *Entry) error
}

// HotKVCache keeps entries in HotKV. It uses the prompt cache (PCACHE.*, with
// hit statistics) when the server is licensed for it and plain strings
// otherwise, so the gateway works against any HotKV server.
type HotKVCache struct {
	client    *hotkv.Client
	namespace string
	ttl       time.Duration
	threshold float64
	prefix    string

	mu       sync.Mutex
	plain    bool                // the server has no prompt-cache license; use GET/SET
	spaces   map[string]struct{} // semantic namespaces created so far
	maxSpace int
}

// NewHotKVCache connects to url and prepares the cache.
func NewHotKVCache(ctx context.Context, url, prefix string, ttl time.Duration, maxSpaces int, threshold float64) (*HotKVCache, error) {
	opts, err := hotkv.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client, err := hotkv.NewClient(opts)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	c := &HotKVCache{
		client: client, namespace: prefix + ":exact", ttl: ttl, prefix: prefix,
		spaces: map[string]struct{}{}, maxSpace: maxSpaces, threshold: threshold,
	}
	err = client.PCacheCreate(ctx, c.namespace, 0, ttl, 0).Err()
	switch {
	case err == nil || strings.Contains(err.Error(), "already exists"):
	case hotkv.IsNoLicenseError(err):
		c.plain = true
	default:
		_ = client.Close()
		return nil, err
	}
	return c, nil
}

// Plain reports whether entries are kept as plain strings (no prompt-cache license).
func (c *HotKVCache) Plain() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.plain
}

// Close releases the connection pool.
func (c *HotKVCache) Close() error { return c.client.Close() }

// Client exposes the connection, for health checks and tests.
func (c *HotKVCache) Client() *hotkv.Client { return c.client }

func (c *HotKVCache) plainKey(key string) string { return c.prefix + ":e:" + key }

func (c *HotKVCache) Get(ctx context.Context, key string) (*Entry, error) {
	var raw string
	var err error
	if c.Plain() {
		raw, err = c.client.Get(ctx, c.plainKey(key)).Result()
	} else {
		raw, err = c.client.PCacheGet(ctx, c.namespace, key).Result()
		if hotkv.IsNoLicenseError(err) {
			c.switchToPlain()
			return c.Get(ctx, key)
		}
	}
	if err == hotkv.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e Entry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return nil, nil // an entry this version cannot read is a miss
	}
	return &e, nil
}

func (c *HotKVCache) switchToPlain() {
	c.mu.Lock()
	c.plain = true
	c.mu.Unlock()
}

func (c *HotKVCache) Set(ctx context.Context, key string, e *Entry, ttl time.Duration) error {
	if !utf8.ValidString(e.Body) {
		return errors.New("response is not valid UTF-8")
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if c.Plain() {
		return c.client.Set(ctx, c.plainKey(key), string(raw), ttl).Err()
	}
	usage := &hotkv.Usage{Model: e.Model, Input: e.PromptTokens, Output: e.CompletionTokens}
	err = c.client.PCacheSet(ctx, c.namespace, key, string(raw), ttl, "", usage).Err()
	if hotkv.IsNoLicenseError(err) {
		c.switchToPlain()
		return c.Set(ctx, key, e, ttl)
	}
	return err
}

// ---- semantic ----

func (c *HotKVCache) ensureSpace(ctx context.Context, space string, dims int) error {
	c.mu.Lock()
	_, known := c.spaces[space]
	over := len(c.spaces) >= c.maxSpace
	c.mu.Unlock()
	if known {
		return nil
	}
	if over {
		return errors.New("too many semantic namespaces")
	}
	th := c.threshold
	err := c.client.SCacheCreate(ctx, space, dims, &hotkv.SCacheCreateOptions{Metric: "cosine", Threshold: &th, TTL: c.ttl}).Err()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	c.mu.Lock()
	c.spaces[space] = struct{}{}
	c.mu.Unlock()
	return nil
}

func (c *HotKVCache) Lookup(ctx context.Context, space string, vector []float32) (string, float64, bool, error) {
	// A lookup never creates a namespace: callers that vary the system prompt or the parameters
	// would otherwise create one per variation. A namespace that does not exist yet is a miss.
	res, err := c.client.SCacheGet(ctx, space, vector, nil).Result()
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	if !res.Hit {
		return "", 0, false, nil
	}
	return res.Response, res.Similarity, true, nil
}

func (c *HotKVCache) Store(ctx context.Context, space, prompt, answer string, vector []float32, ttl time.Duration, usage *Entry) error {
	if err := c.ensureSpace(ctx, space, len(vector)); err != nil {
		return err
	}
	var u *hotkv.Usage
	if usage != nil && usage.Model != "" {
		u = &hotkv.Usage{Model: usage.Model, Input: usage.PromptTokens, Output: usage.CompletionTokens}
	}
	return c.client.SCacheSet(ctx, space, prompt, answer, vector, ttl, u).Err()
}

// SemanticAvailable checks that the server is licensed for the semantic cache.
func (c *HotKVCache) SemanticAvailable(ctx context.Context) error {
	probe := c.prefix + ":probe"
	err := c.client.SCacheCreate(ctx, probe, 2, nil).Err()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	_ = c.client.SCacheDrop(ctx, probe).Err()
	return nil
}
