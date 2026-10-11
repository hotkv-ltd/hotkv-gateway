// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Version is set at build time.
var Version = "1.1.1"

// Header the gateway adds to every cacheable response: HIT, SEMANTIC-HIT, MISS, COALESCED or BYPASS.
const cacheHeader = "X-Hotkv-Cache"

type kind int

const (
	kindNone kind = iota
	kindChat
	kindCompletions
	kindEmbeddings
	kindMessages
)

func kindOf(path string) kind {
	p := strings.TrimSuffix(path, "/")
	switch {
	case strings.HasSuffix(p, "/v1/messages"):
		return kindMessages // Anthropic's Messages API; /messages/batches and /messages/count_tokens are not matched
	case strings.HasSuffix(p, "/chat/completions"):
		return kindChat
	case strings.HasSuffix(p, "/embeddings"):
		return kindEmbeddings
	case strings.HasSuffix(p, "/completions"):
		return kindCompletions
	}
	return kindNone
}

// Gateway is the http.Handler of the caching proxy.
type Gateway struct {
	cfg           Config
	log           *slog.Logger
	cache         Cache
	sem           Semantic // nil when the semantic cache is off
	client        *http.Client
	proxy         *httputil.ReverseProxy
	mode          string
	fl            flights
	slots         chan struct{}  // bounds the requests whose bodies are held in memory
	bg            sync.WaitGroup // cache writes still running after their response was sent
	embedFailedAt atomic.Int64   // unix nanos of the last embeddings failure, for the semantic circuit breaker

	Metrics Metrics
}

// Drain waits for cache writes that run after a response was sent. Call it before shutting down.
func (g *Gateway) Drain() { g.bg.Wait() }

// background runs fn after the response has gone out, tracked so Drain can wait for it.
func (g *Gateway) background(fn func()) {
	g.bg.Add(1)
	go func() {
		defer g.bg.Done()
		defer func() {
			if p := recover(); p != nil {
				g.log.Error("background cache write panicked", "panic", fmt.Sprint(p))
			}
		}()
		fn()
	}()
}

// New builds a gateway. mode only labels the metrics ("prompt-cache" or "plain").
func New(cfg Config, cache Cache, sem Semantic, mode string, log *slog.Logger) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if !cfg.Semantic {
		sem = nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = cfg.UpstreamTimeout
	tr.MaxIdleConnsPerHost = 64
	g := &Gateway{
		cfg: cfg, log: log, cache: cache, sem: sem, mode: mode,
		slots: make(chan struct{}, cfg.MaxInFlight),
		client: &http.Client{
			Transport:     tr,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(cfg.Upstream)
			pr.Out.Host = cfg.Upstream.Host
			if cfg.UpstreamAPIKey != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+cfg.UpstreamAPIKey)
			}
		},
		FlushInterval: -1,
		Transport:     tr,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			g.Metrics.UpstreamErrors.Add(1)
			g.log.Warn("upstream error", "error", err)
			writeError(w, http.StatusBadGateway, "upstream unreachable")
		},
	}
	return g, nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, segment := range strings.Split(r.URL.Path, "/") {
		if segment == ".." {
			// a path that climbs out of the upstream's base path is never legitimate
			writeError(w, http.StatusBadRequest, "invalid path")
			return
		}
	}
	switch {
	case r.URL.Path == "/_hotkv/health":
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","cache":%q,"semantic":%t,"version":%q}`+"\n", g.mode, g.sem != nil, Version)
	case r.URL.Path == "/_hotkv/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		g.Metrics.write(w, g.mode)
	case r.Method == http.MethodPost && kindOf(r.URL.Path) != kindNone:
		g.cached(w, r)
	default:
		g.proxy.ServeHTTP(w, r)
	}
}

// policy says what may be done with one request.
type policy struct {
	lookup bool // look for an answer in the cache
	store  bool // keep the upstream answer
}

// decide applies the caching rules. ok=false means send the request upstream untouched.
func (g *Gateway) decide(r *http.Request, req map[string]any) (p policy, ok bool) {
	p = policy{lookup: true, store: true}
	directives := strings.ToLower(r.Header.Get("X-Hotkv-Cache-Control") + "," + r.Header.Get("Cache-Control"))
	switch {
	case strings.Contains(directives, "bypass"):
		return p, false
	case strings.Contains(directives, "no-cache"):
		p.lookup = false
	}
	if strings.Contains(directives, "no-store") {
		p.store = false
	}
	model, _ := req["model"].(string)
	if !g.cfg.modelCached(model) {
		return p, false
	}
	if n, has := number(req["n"]); has && n > 1 {
		return p, false
	}
	if g.cfg.MaxTemperature >= 0 {
		if t, has := number(req["temperature"]); has && t > g.cfg.MaxTemperature {
			return p, false
		}
	}
	return p, true
}

func number(v any) (float64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	f, err := n.Float64()
	return f, err == nil
}

func isStream(req map[string]any) bool {
	b, _ := req["stream"].(bool)
	return b
}

func (g *Gateway) cached(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "too many requests in flight")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, g.cfg.MaxRequestBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		} else {
			writeError(w, http.StatusBadRequest, "cannot read the request body")
		}
		return
	}
	req, perr := parseBody(body)
	pol, ok := policy{}, false
	if perr == nil {
		pol, ok = g.decide(r, req)
	}
	if !ok {
		g.Metrics.Bypasses.Add(1)
		g.bypass(w, r, body)
		return
	}

	k := kindOf(r.URL.Path)
	tenant := tenantID(r, g.cfg.Scope, g.cfg.TenantHeaders)
	key := requestKey(r.URL.Path, r.URL.RawQuery, tenant+behaviourSalt(r), req)
	stream := isStream(req)
	// The cache key is made from what the caller sent; what goes upstream may carry the provider's
	// automatic prompt caching on top.
	upBody := body
	if g.cfg.AnthropicAutoCache && k == kindMessages {
		if injected, did := withAutoCache(body, g.cfg.AnthropicCacheTTL); did {
			upBody = injected
		}
	}
	var sx *semCtx
	if g.sem != nil && k == kindChat {
		sx = g.semanticContext(r, req, tenant)
	}

	if pol.lookup {
		if e := g.lookup(r.Context(), key); e != nil {
			g.serveEntry(w, e, "HIT")
			g.Metrics.Hits.Add(1)
			g.Metrics.TokensSaved.Add(e.PromptTokens + e.CompletionTokens)
			g.logRequest(r, "HIT", req, start)
			return
		}
		if sx != nil {
			if answer, ok := g.semanticLookup(r.Context(), snapshot(r), sx); ok {
				e := synthesize(req, stream, answer)
				g.serveEntry(w, e, "SEMANTIC-HIT")
				g.Metrics.SemanticHits.Add(1)
				g.Metrics.TokensSaved.Add(e.PromptTokens + e.CompletionTokens)
				g.logRequest(r, "SEMANTIC-HIT", req, start)
				return
			}
		}
	}

	state := "MISS"
	if stream {
		g.relayStream(w, r, upBody, key, k, pol, sx)
	} else {
		state = g.relayJSON(w, r, upBody, key, k, pol, sx)
	}
	g.logRequest(r, state, req, start)
}

func (g *Gateway) logRequest(r *http.Request, state string, req map[string]any, start time.Time) {
	model, _ := req["model"].(string)
	g.log.Info("request", "path", r.URL.Path, "model", model, "cache", state, "ms", time.Since(start).Milliseconds())
}

// lookup reads the cache and never fails the request: a cache that is down or slow is a miss.
func (g *Gateway) lookup(ctx context.Context, key string) *Entry {
	ctx, cancel := context.WithTimeout(ctx, g.cfg.CacheTimeout)
	defer cancel()
	e, err := g.cache.Get(ctx, key)
	if err != nil {
		g.Metrics.CacheErrors.Add(1)
		g.log.Warn("cache read failed", "error", err)
		return nil
	}
	return e
}

func (g *Gateway) serveEntry(w http.ResponseWriter, e *Entry, state string) {
	h := w.Header()
	if e.ContentType != "" {
		h.Set("Content-Type", e.ContentType)
	}
	h.Set(cacheHeader, state)
	if e.Stream {
		h.Set("Cache-Control", "no-cache")
	}
	h.Set("Content-Length", strconv.Itoa(len(e.Body)))
	w.WriteHeader(e.Status)
	_, _ = io.WriteString(w, e.Body)
}

// ---- talking to the upstream ----

func (g *Gateway) upstreamRequest(ctx context.Context, method, path, rawQuery string, header http.Header, body []byte) (*http.Request, error) {
	u := *g.cfg.Upstream
	u.Path = strings.TrimSuffix(u.Path, "/") + path
	u.RawPath = ""
	switch {
	case u.RawQuery == "":
		u.RawQuery = rawQuery
	case rawQuery != "":
		u.RawQuery += "&" + rawQuery // a query configured on the upstream stays
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for name, values := range header {
		switch http.CanonicalHeaderKey(name) {
		case "Host", "Content-Length", "Accept-Encoding":
			continue
		}
		if hopByHop(name) {
			continue
		}
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	if g.cfg.UpstreamAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+g.cfg.UpstreamAPIKey)
	}
	return req, nil
}

func hopByHop(name string) bool {
	switch http.CanonicalHeaderKey(name) {
	case "Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	}
	return false
}

const maxUpstreamBody = 128 << 20

// call sends a request upstream and reads the whole answer. It is detached from the
// caller's context so that a client giving up does not fail callers that share the result.
func (g *Gateway) call(parent context.Context, method, path, rawQuery string, header http.Header, body []byte) *result {
	return g.callWith(context.WithoutCancel(parent), g.cfg.UpstreamTimeout, method, path, rawQuery, header, body)
}

// callWith is call with the caller's own context and time limit.
func (g *Gateway) callWith(parent context.Context, timeout time.Duration, method, path, rawQuery string, header http.Header, body []byte) (res *result) {
	defer func() {
		if p := recover(); p != nil {
			res = &result{err: fmt.Errorf("internal error: %v", p)}
		}
	}()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := g.upstreamRequest(ctx, method, path, rawQuery, header, body)
	if err != nil {
		return &result{err: err}
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return &result{err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBody+1))
	if err != nil {
		return &result{err: err}
	}
	if len(data) > maxUpstreamBody {
		return &result{err: errors.New("upstream response is too large")}
	}
	return &result{status: resp.StatusCode, header: resp.Header, body: data}
}

func (g *Gateway) bypass(w http.ResponseWriter, r *http.Request, body []byte) {
	if isStreamBody(body) {
		g.relayStream(w, r, body, "", kindNone, policy{}, nil)
		return
	}
	res := g.call(r.Context(), r.Method, r.URL.Path, r.URL.RawQuery, r.Header, body)
	g.writeResult(w, res, "BYPASS", false)
}

func isStreamBody(body []byte) bool {
	req, err := parseBody(body)
	return err == nil && isStream(req)
}

// writeResult sends a complete upstream answer. shared is true when other callers got the same
// upstream call; per-connection headers such as cookies are then not passed on.
func (g *Gateway) writeResult(w http.ResponseWriter, res *result, state string, shared bool) {
	if res.err != nil {
		g.Metrics.UpstreamErrors.Add(1)
		g.log.Warn("upstream error", "error", res.err)
		writeError(w, http.StatusBadGateway, "upstream unreachable")
		return
	}
	copyHeaders(w.Header(), res.header)
	if shared {
		w.Header().Del("Set-Cookie")
	}
	w.Header().Set(cacheHeader, state)
	w.Header().Set("Content-Length", strconv.Itoa(len(res.body)))
	w.WriteHeader(res.status)
	_, _ = w.Write(res.body)
	// the body is complete (Content-Length): let the caller have it before any cache work starts
	_ = http.NewResponseController(w).Flush()
}

func copyHeaders(dst, src http.Header) {
	for name, values := range src {
		if hopByHop(name) || http.CanonicalHeaderKey(name) == "Content-Length" {
			continue
		}
		dst.Del(name)
		for _, v := range values {
			dst.Add(name, v)
		}
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"message": message, "type": "gateway_error"}})
	_, _ = w.Write(append(b, '\n'))
}

// ---- non-streaming ----

func (g *Gateway) relayJSON(w http.ResponseWriter, r *http.Request, body []byte, key string, k kind, pol policy, sx *semCtx) string {
	res, shared, err := g.fl.do(r.Context(), key, func() *result {
		return g.call(r.Context(), r.Method, r.URL.Path, r.URL.RawQuery, r.Header, body)
	})
	if err != nil {
		return "CANCELLED" // the caller went away while waiting
	}
	state := "MISS"
	if shared {
		state = "COALESCED"
		g.Metrics.Coalesced.Add(1)
	} else {
		g.Metrics.Misses.Add(1)
	}
	g.writeResult(w, res, state, shared)
	if !shared && res.err == nil && res.status == http.StatusOK {
		if u, found := usageOf(k, res.body); found {
			g.Metrics.addUsage(u)
		}
		if pol.store {
			g.storeJSON(r.Context(), snapshot(r), key, k, res, sx)
		}
	}
	return state
}

func (g *Gateway) storeJSON(ctx context.Context, ri reqInfo, key string, k kind, res *result, sx *semCtx) {
	if len(res.body) > g.cfg.MaxEntryBytes {
		return
	}
	info, ok := inspectJSON(k, res.body)
	if !ok {
		return
	}
	e := &Entry{
		Status: http.StatusOK, ContentType: res.header.Get("Content-Type"), Body: string(res.body),
		Model: info.model, PromptTokens: info.promptTokens, CompletionTokens: info.completionTokens,
	}
	g.put(ctx, key, e)
	if sx != nil && info.plainText {
		g.semanticStore(ctx, ri, sx, info.text, e)
	}
}

func (g *Gateway) put(ctx context.Context, key string, e *Entry) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.CacheTimeout)
	defer cancel()
	if err := g.cache.Set(ctx, key, e, g.cfg.TTL); err != nil {
		g.Metrics.CacheErrors.Add(1)
		g.log.Warn("cache write failed", "error", err)
		return
	}
	g.Metrics.Stored.Add(1)
}

// ---- streaming ----

func (g *Gateway) relayStream(w http.ResponseWriter, r *http.Request, body []byte, key string, k kind, pol policy, sx *semCtx) {
	// A streaming call lives as long as the caller: if they hang up, the upstream call is cancelled
	// and the tokens that were not yet generated are not spent. It is also cut when the upstream
	// goes quiet for StreamIdleTimeout.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	req, err := g.upstreamRequest(ctx, r.Method, r.URL.Path, r.URL.RawQuery, r.Header, body)
	if err != nil {
		writeError(w, http.StatusBadGateway, "cannot build the upstream request")
		return
	}
	resp, err := g.client.Do(req)
	if err != nil {
		if r.Context().Err() == nil {
			g.Metrics.UpstreamErrors.Add(1)
			g.log.Warn("upstream error", "error", err)
			writeError(w, http.StatusBadGateway, "upstream unreachable")
		}
		return
	}
	defer resp.Body.Close()
	if key != "" {
		g.Metrics.Misses.Add(1)
	}
	idle := time.AfterFunc(g.cfg.StreamIdleTimeout, cancel)
	defer idle.Stop()

	copyHeaders(w.Header(), resp.Header)
	state := "MISS"
	if key == "" {
		state = "BYPASS"
	}
	w.Header().Set(cacheHeader, state)
	w.WriteHeader(resp.StatusCode)

	capture := key != "" && resp.StatusCode == http.StatusOK && strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	store := capture && pol.store
	var captured bytes.Buffer
	flusher, _ := w.(http.Flusher)
	chunk := make([]byte, 32<<10)
	var readErr error
	for {
		n, rerr := resp.Body.Read(chunk)
		idle.Reset(g.cfg.StreamIdleTimeout)
		if n > 0 {
			if _, werr := w.Write(chunk[:n]); werr != nil {
				return // the caller went away
			}
			if flusher != nil {
				flusher.Flush()
			}
			if capture {
				if captured.Len()+n > g.cfg.MaxEntryBytes {
					capture, store = false, false
					captured.Reset()
				} else {
					captured.Write(chunk[:n])
				}
			}
		}
		if rerr != nil {
			readErr = rerr
			break
		}
	}
	if readErr != io.EOF {
		if r.Context().Err() != nil {
			return // the caller hung up
		}
		// the upstream broke the stream: do not let the caller mistake it for a finished answer
		g.Metrics.UpstreamErrors.Add(1)
		g.log.Warn("upstream stream failed", "error", readErr)
		panic(http.ErrAbortHandler)
	}
	if !capture {
		return
	}
	st := parseStreamFor(k, captured.Bytes())
	if st.complete {
		g.Metrics.addUsage(st.usage)
	}
	if !store || !st.complete {
		return
	}
	e := &Entry{
		Status: http.StatusOK, ContentType: resp.Header.Get("Content-Type"), Stream: true, Body: captured.String(),
		Model: st.model, PromptTokens: st.usage.promptTotal(), CompletionTokens: st.usage.output,
	}
	// The caller has the whole stream; keep it from waiting for the end of the response while the cache is written.
	ri := snapshot(r)
	detached := context.WithoutCancel(r.Context())
	g.background(func() {
		g.put(detached, key, e)
		if sx != nil && st.plainText {
			g.semanticStore(detached, ri, sx, st.text, e)
		}
	})
}
