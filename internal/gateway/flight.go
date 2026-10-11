// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"context"
	"net/http"
	"sync"
)

// result is a complete upstream response.
type result struct {
	status int
	header http.Header
	body   []byte
	err    error
}

type call struct {
	done chan struct{}
	res  *result
}

// flights makes identical requests that arrive together share one upstream call.
type flights struct {
	mu    sync.Mutex
	calls map[string]*call
}

// do runs fn once per key at a time. Callers that arrive while it runs wait for
// its result (shared=true) or give up when their own context ends.
func (f *flights) do(ctx context.Context, key string, fn func() *result) (res *result, shared bool, err error) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]*call{}
	}
	if c, ok := f.calls[key]; ok {
		f.mu.Unlock()
		select {
		case <-c.done:
			return c.res, true, nil
		case <-ctx.Done():
			return nil, true, ctx.Err()
		}
	}
	c := &call{done: make(chan struct{})}
	f.calls[key] = c
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		delete(f.calls, key)
		f.mu.Unlock()
		close(c.done)
	}()
	c.res = fn()
	return c.res, false, nil
}
