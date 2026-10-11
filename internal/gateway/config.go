// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

// Package gateway is an OpenAI-compatible caching reverse proxy backed by HotKV.
package gateway

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Cache scopes.
const (
	// ScopeKey keeps a separate cache per caller credential (the safe default).
	ScopeKey = "key"
	// ScopeGlobal shares answers between all callers.
	ScopeGlobal = "global"
)

// Config is everything the gateway needs to run.
type Config struct {
	Listen   string   // address to listen on
	Upstream *url.URL // the LLM API, for example https://api.openai.com

	// UpstreamAPIKey, when set, replaces the caller's Authorization header, so
	// callers need no key of their own. All callers then share one cache scope.
	UpstreamAPIKey string

	HotKVURL string // where the cache lives
	Prefix   string // key and namespace prefix on the HotKV server

	TTL             time.Duration // how long an answer stays cached
	MaxTemperature  float64       // requests asking for more randomness than this are not cached; negative disables the rule
	MaxEntryBytes   int           // larger responses are not cached
	MaxRequestBytes int64         // larger requests are refused
	Scope           string        // ScopeKey or ScopeGlobal
	CacheTimeout    time.Duration // longest the cache may take before the request goes upstream anyway
	UpstreamTimeout time.Duration // longest a non-streaming upstream call may take
	CachedModels    []string      // when set, only these models are cached

	// TenantHeaders are request headers that identify the caller, besides the standard
	// credential headers. Their values are part of every cache key.
	TenantHeaders     []string
	StreamIdleTimeout time.Duration // a stream that sends nothing for this long is cut
	EmbeddingTimeout  time.Duration // longest the semantic cache waits for an embedding
	MaxInFlight       int           // most requests held in memory at once; more get a 503

	// AnthropicAutoCache turns on Anthropic's prompt caching for Messages API requests that do not use it.
	AnthropicAutoCache bool
	AnthropicCacheTTL  string // "5m" or "1h": how long the provider keeps what it cached

	Semantic          bool    // also answer by meaning
	EmbeddingModel    string  // model used to embed the prompt for the semantic cache
	SemanticThreshold float64 // similarity needed for a semantic hit
	MaxSemanticSpaces int     // most semantic namespaces to create
}

// DefaultConfig holds the defaults of every setting.
func DefaultConfig() Config {
	return Config{
		Listen:            "127.0.0.1:8080",
		HotKVURL:          "hotkv://127.0.0.1:6379",
		Prefix:            "gw",
		TTL:               time.Hour,
		MaxTemperature:    -1,
		MaxEntryBytes:     1 << 20,
		MaxRequestBytes:   32 << 20,
		Scope:             ScopeKey,
		CacheTimeout:      250 * time.Millisecond,
		UpstreamTimeout:   10 * time.Minute,
		StreamIdleTimeout: 5 * time.Minute,
		EmbeddingTimeout:  5 * time.Second,
		MaxInFlight:       1024,
		AnthropicCacheTTL: "5m",
		EmbeddingModel:    "text-embedding-3-small",
		SemanticThreshold: 0.92,
		MaxSemanticSpaces: 1000,
	}
}

// Validate reports the first problem with the settings.
func (c *Config) Validate() error {
	if c.Upstream == nil || c.Upstream.Host == "" || (c.Upstream.Scheme != "http" && c.Upstream.Scheme != "https") {
		return errors.New("upstream must be an http:// or https:// URL, for example https://api.openai.com")
	}
	if c.Scope != ScopeKey && c.Scope != ScopeGlobal {
		return fmt.Errorf("scope must be %q or %q", ScopeKey, ScopeGlobal)
	}
	if c.TTL <= 0 {
		return errors.New("ttl must be positive")
	}
	if c.MaxEntryBytes < 1024 {
		return errors.New("max entry size must be at least 1024 bytes")
	}
	if c.MaxRequestBytes < 1024 {
		return errors.New("max request size must be at least 1024 bytes")
	}
	if c.CacheTimeout <= 0 || c.UpstreamTimeout <= 0 || c.StreamIdleTimeout <= 0 || c.EmbeddingTimeout <= 0 {
		return errors.New("timeouts must be positive")
	}
	if c.AnthropicCacheTTL != "5m" && c.AnthropicCacheTTL != "1h" {
		return errors.New("anthropic cache ttl must be 5m or 1h")
	}
	if c.MaxInFlight < 1 {
		return errors.New("max in-flight requests must be at least 1")
	}
	if c.Semantic && (c.SemanticThreshold <= 0 || c.SemanticThreshold > 1) {
		return errors.New("semantic threshold must be in (0, 1]")
	}
	if c.Semantic && c.EmbeddingModel == "" {
		return errors.New("semantic caching needs an embedding model")
	}
	if c.Prefix == "" || strings.ContainsAny(c.Prefix, " \t\r\n") {
		return errors.New("prefix must be non-empty and contain no whitespace")
	}
	return nil
}

func (c *Config) modelCached(model string) bool {
	if len(c.CachedModels) == 0 {
		return true
	}
	for _, m := range c.CachedModels {
		if m == model {
			return true
		}
	}
	return false
}
