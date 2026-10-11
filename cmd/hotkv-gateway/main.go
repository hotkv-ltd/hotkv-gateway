// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

// Command hotkv-gateway is an OpenAI-compatible caching proxy backed by HotKV.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hotkv-ltd/hotkv-gateway/internal/gateway"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "hotkv-gateway:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg := gateway.DefaultConfig()
	fs := flag.NewFlagSet("hotkv-gateway", flag.ContinueOnError)
	upstream := fs.String("upstream", envOr("HOTKV_GATEWAY_UPSTREAM", "https://api.openai.com"), "LLM API to forward to ($HOTKV_GATEWAY_UPSTREAM)")
	fs.StringVar(&cfg.Listen, "listen", envOr("HOTKV_GATEWAY_LISTEN", cfg.Listen), "address to listen on ($HOTKV_GATEWAY_LISTEN)")
	fs.StringVar(&cfg.HotKVURL, "hotkv-url", envOr("HOTKV_URL", cfg.HotKVURL), "HotKV server holding the cache, hotkv:// hotkvs:// redis:// ($HOTKV_URL)")
	fs.StringVar(&cfg.Prefix, "prefix", cfg.Prefix, "key and namespace prefix on the HotKV server")
	fs.DurationVar(&cfg.TTL, "ttl", cfg.TTL, "how long an answer stays cached")
	fs.StringVar(&cfg.Scope, "scope", cfg.Scope, `"key": a cache per caller credential; "global": share answers between callers`)
	fs.Float64Var(&cfg.MaxTemperature, "max-temperature", cfg.MaxTemperature, "do not cache requests with a higher temperature (negative: cache all)")
	models := fs.String("models", "", "comma-separated models to cache (default: all)")
	fs.IntVar(&cfg.MaxEntryBytes, "max-entry-bytes", cfg.MaxEntryBytes, "largest response to cache")
	fs.Int64Var(&cfg.MaxRequestBytes, "max-request-bytes", cfg.MaxRequestBytes, "largest request body accepted")
	fs.DurationVar(&cfg.CacheTimeout, "cache-timeout", cfg.CacheTimeout, "longest a cache read or write may take before the request goes on without it")
	fs.DurationVar(&cfg.UpstreamTimeout, "upstream-timeout", cfg.UpstreamTimeout, "longest to wait for the upstream")
	fs.BoolVar(&cfg.Semantic, "semantic", false, "also answer by meaning (needs a HotKV license that includes the semantic cache)")
	fs.StringVar(&cfg.EmbeddingModel, "embedding-model", cfg.EmbeddingModel, "model used to embed prompts for the semantic cache")
	fs.Float64Var(&cfg.SemanticThreshold, "semantic-threshold", cfg.SemanticThreshold, "similarity needed for a semantic hit, 0 to 1")
	tenantHeaders := fs.String("tenant-headers", "", "extra request headers that identify the caller, comma-separated (Authorization, X-Api-Key, Api-Key, X-Goog-Api-Key and Cookie always count)")
	fs.DurationVar(&cfg.StreamIdleTimeout, "stream-idle-timeout", cfg.StreamIdleTimeout, "cut a stream that sends nothing for this long")
	fs.DurationVar(&cfg.EmbeddingTimeout, "embedding-timeout", cfg.EmbeddingTimeout, "longest the semantic cache waits for an embedding")
	fs.IntVar(&cfg.MaxInFlight, "max-in-flight", cfg.MaxInFlight, "most requests handled at once; more get a 503")
	fs.BoolVar(&cfg.AnthropicAutoCache, "anthropic-auto-cache", false, "turn on Anthropic prompt caching for Messages API requests that do not use it: breakpoints on the tools and the system prompt, and on the conversation once it has an assistant turn")
	fs.StringVar(&cfg.AnthropicCacheTTL, "anthropic-cache-ttl", cfg.AnthropicCacheTTL, "how long Anthropic keeps what it cached: 5m or 1h (with --anthropic-auto-cache)")
	logLevel := fs.String("log-level", "info", "debug, info, warn or error")
	version := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *version {
		fmt.Println("hotkv-gateway", gateway.Version)
		return nil
	}

	// The key is taken from the environment only, so it never shows up in a process list.
	cfg.UpstreamAPIKey = os.Getenv("HOTKV_GATEWAY_UPSTREAM_API_KEY")
	u, err := url.Parse(*upstream)
	if err != nil {
		return fmt.Errorf("upstream: %w", err)
	}
	cfg.Upstream = u
	for _, h := range strings.Split(*tenantHeaders, ",") {
		if h = strings.TrimSpace(h); h != "" {
			cfg.TenantHeaders = append(cfg.TenantHeaders, h)
		}
	}
	if *models != "" {
		for _, m := range strings.Split(*models, ",") {
			if m = strings.TrimSpace(m); m != "" {
				cfg.CachedModels = append(cfg.CachedModels, m)
			}
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("log-level: %w", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	cache, err := gateway.NewHotKVCache(connectCtx, cfg.HotKVURL, cfg.Prefix, cfg.TTL, cfg.MaxSemanticSpaces, cfg.SemanticThreshold)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot use the HotKV server at %s: %w", redact(cfg.HotKVURL), err)
	}
	defer cache.Close()
	mode := "prompt-cache"
	if cache.Plain() {
		mode = "plain"
		log.Info("the HotKV server has no prompt-cache license; caching with plain keys (no cache statistics)")
	}
	var sem gateway.Semantic
	if cfg.Semantic {
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := cache.SemanticAvailable(probeCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("the semantic cache is not available on this HotKV server: %w", err)
		}
		sem = cache
	}

	gw, err := gateway.New(cfg, cache, sem, mode, log)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           gw,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", cfg.Listen, "upstream", cfg.Upstream.String(), "cache", mode, "semantic", cfg.Semantic, "scope", cfg.Scope, "ttl", cfg.TTL.String())

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdown)
	gw.Drain() // cache writes that were still running
	return err
}

// redact hides a password in a connection URL.
func redact(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	if _, has := u.User.Password(); has {
		u.User = url.UserPassword(u.User.Username(), "xxxxx")
	}
	return u.String()
}
