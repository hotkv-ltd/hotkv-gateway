// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Metrics counts what the gateway did, for /_hotkv/metrics (Prometheus text format).
type Metrics struct {
	Hits           atomic.Uint64 // answered from the exact cache
	SemanticHits   atomic.Uint64 // answered from the semantic cache
	Misses         atomic.Uint64 // sent upstream and eligible for caching
	Bypasses       atomic.Uint64 // sent upstream and not eligible
	Coalesced      atomic.Uint64 // waited for an identical request already in flight
	Stored         atomic.Uint64 // responses written to the cache
	UpstreamErrors atomic.Uint64 // upstream unreachable
	CacheErrors    atomic.Uint64 // cache read or write failed (the request went on regardless)
	TokensSaved    atomic.Uint64 // prompt and completion tokens the hits did not spend
}

func (m *Metrics) write(w io.Writer, mode string) {
	counter := func(name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	fmt.Fprintf(w, "# HELP hotkv_gateway_info The gateway's cache mode.\n# TYPE hotkv_gateway_info gauge\nhotkv_gateway_info{cache=%q} 1\n", mode)
	fmt.Fprintf(w, "# HELP hotkv_gateway_requests_total Cacheable requests by result.\n# TYPE hotkv_gateway_requests_total counter\n")
	fmt.Fprintf(w, "hotkv_gateway_requests_total{result=\"hit\"} %d\n", m.Hits.Load())
	fmt.Fprintf(w, "hotkv_gateway_requests_total{result=\"semantic_hit\"} %d\n", m.SemanticHits.Load())
	fmt.Fprintf(w, "hotkv_gateway_requests_total{result=\"miss\"} %d\n", m.Misses.Load())
	fmt.Fprintf(w, "hotkv_gateway_requests_total{result=\"bypass\"} %d\n", m.Bypasses.Load())
	counter("hotkv_gateway_coalesced_total", "Requests that shared an identical request already in flight.", m.Coalesced.Load())
	counter("hotkv_gateway_stored_total", "Responses written to the cache.", m.Stored.Load())
	counter("hotkv_gateway_upstream_errors_total", "Requests the upstream could not be reached for.", m.UpstreamErrors.Load())
	counter("hotkv_gateway_cache_errors_total", "Cache reads or writes that failed.", m.CacheErrors.Load())
	counter("hotkv_gateway_tokens_saved_total", "Prompt and completion tokens that cache hits did not spend.", m.TokensSaved.Load())
}
