// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// ignoredFields do not change what the model answers, so requests that differ
// only in them share a cache entry.
var ignoredFields = map[string]bool{
	"user":              true,
	"metadata":          true,
	"store":             true,
	"prompt_cache_key":  true,
	"safety_identifier": true,
}

// keyVersion is bumped when the key derivation changes, so old entries are not served for new keys.
const keyVersion = "v1"

// defaultTenantHeaders identify who is calling. Their values are hashed into the cache key,
// so one caller never receives a response that was generated for another. A deployment whose
// upstream authenticates on another header adds it with Config.TenantHeaders.
var defaultTenantHeaders = []string{
	"Authorization", "Proxy-Authorization", "X-Api-Key", "Api-Key", "Openai-Organization", "Openai-Project",
	"X-Goog-Api-Key", "Cookie",
}

// behaviourHeaders change what an API does with a request (a beta feature, an API version), so they are
// part of every cache key: an answer produced under one setting is never served under another.
var behaviourHeaders = []string{"Anthropic-Version", "Anthropic-Beta", "Openai-Beta"}

func behaviourSalt(r *http.Request) string {
	var b strings.Builder
	for _, name := range behaviourHeaders {
		for _, v := range r.Header.Values(name) {
			fmt.Fprintf(&b, "|%s=%s", strings.ToLower(name), v)
		}
	}
	return b.String()
}

func tenantHeaderNames(extra []string) []string {
	return append(append([]string(nil), defaultTenantHeaders...), extra...)
}

// tenantID is a short digest of the caller's credentials ("" for the global scope).
func tenantID(r *http.Request, scope string, extra []string) string {
	if scope == ScopeGlobal {
		return ""
	}
	h := sha256.New()
	seen := false
	for _, name := range tenantHeaderNames(extra) {
		values := r.Header.Values(name)
		if len(values) == 0 {
			continue
		}
		seen = true
		for _, v := range values {
			fmt.Fprintf(h, "%s=%s\n", strings.ToLower(name), v)
		}
	}
	if !seen {
		return "anonymous"
	}
	return hex.EncodeToString(h.Sum(nil))[:24]
}

// parseBody decodes a JSON request body, keeping numbers exact.
func parseBody(body []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("request body is not a JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("request body has data after the JSON object")
	}
	return obj, nil
}

// canonical writes v as JSON with sorted keys and normalised numbers, so two
// requests that mean the same thing produce the same bytes.
func canonical(buf *bytes.Buffer, v any) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			buf.Write(kb)
			buf.WriteByte(':')
			canonical(buf, t[k])
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			canonical(buf, e)
		}
		buf.WriteByte(']')
	case json.Number:
		buf.WriteString(normaliseNumber(t))
	default:
		b, _ := json.Marshal(t)
		buf.Write(b)
	}
}

func normaliseNumber(n json.Number) string {
	if i, err := strconv.ParseInt(string(n), 10, 64); err == nil {
		return strconv.FormatInt(i, 10)
	}
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return string(n)
	}
	if f == float64(int64(f)) && f > -1e15 && f < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// requestKey derives the cache key of a request: it covers the endpoint, the
// query string (an Azure api-version, for instance), the caller and every body
// field that can change the answer.
func requestKey(path, rawQuery, tenant string, body map[string]any) string {
	cleaned := make(map[string]any, len(body))
	for k, v := range body {
		if !ignoredFields[k] {
			cleaned[k] = v
		}
	}
	var buf bytes.Buffer
	canonical(&buf, cleaned)
	sum := sha256.New()
	fmt.Fprintf(sum, "%s\n%s\n%s\n%s\n", keyVersion, path, rawQuery, tenant)
	sum.Write(buf.Bytes())
	return hex.EncodeToString(sum.Sum(nil))
}
