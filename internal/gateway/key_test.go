// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package gateway

import (
	"net/http"
	"testing"
)

func mustParse(t *testing.T, s string) map[string]any {
	t.Helper()
	m, err := parseBody([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestKeyIgnoresOrderNumberFormattingAndIrrelevantFields(t *testing.T) {
	a := requestKey("/v1/chat/completions", "", "t", mustParse(t, `{"model":"m","temperature":1.0,"top_p":0.50,"user":"x","metadata":{"a":1}}`))
	b := requestKey("/v1/chat/completions", "", "t", mustParse(t, `{"top_p":0.5,"temperature":1,"model":"m"}`))
	if a != b {
		t.Fatal("equivalent requests got different keys")
	}
}

func TestKeyChangesWithEverythingThatMatters(t *testing.T) {
	base := mustParse(t, `{"model":"m","messages":[{"role":"user","content":"a"}]}`)
	k := requestKey("/v1/chat/completions", "", "t", base)
	cases := map[string]string{
		"path":    requestKey("/v1/completions", "", "t", base),
		"query":   requestKey("/v1/chat/completions", "api-version=2", "t", base),
		"tenant":  requestKey("/v1/chat/completions", "", "u", base),
		"model":   requestKey("/v1/chat/completions", "", "t", mustParse(t, `{"model":"n","messages":[{"role":"user","content":"a"}]}`)),
		"content": requestKey("/v1/chat/completions", "", "t", mustParse(t, `{"model":"m","messages":[{"role":"user","content":"b"}]}`)),
		"stream":  requestKey("/v1/chat/completions", "", "t", mustParse(t, `{"model":"m","stream":true,"messages":[{"role":"user","content":"a"}]}`)),
	}
	for what, other := range cases {
		if other == k {
			t.Errorf("changing the %s did not change the key", what)
		}
	}
}

func TestLargeIntegersKeepTheirValue(t *testing.T) {
	a := requestKey("p", "", "", mustParse(t, `{"seed":9007199254740993}`))
	b := requestKey("p", "", "", mustParse(t, `{"seed":9007199254740992}`))
	if a == b {
		t.Fatal("seeds that differ by one collided")
	}
}

func TestTenantIDHashesCredentials(t *testing.T) {
	r1, _ := http.NewRequest("POST", "http://x", nil)
	r1.Header.Set("Authorization", "Bearer secret-1")
	r2, _ := http.NewRequest("POST", "http://x", nil)
	r2.Header.Set("Authorization", "Bearer secret-2")
	a, b := tenantID(r1, ScopeKey, nil), tenantID(r2, ScopeKey, nil)
	if a == b || a == "" || len(a) != 24 {
		t.Fatalf("tenant ids %q %q", a, b)
	}
	if tenantID(r1, ScopeGlobal, nil) != "" {
		t.Fatal("global scope must have no tenant")
	}
	none, _ := http.NewRequest("POST", "http://x", nil)
	if tenantID(none, ScopeKey, nil) != "anonymous" {
		t.Fatal("no credentials should be the anonymous tenant")
	}
}

func TestInspectRefusesWhatShouldNotBeKept(t *testing.T) {
	good := `{"model":"m","choices":[{"finish_reason":"stop","message":{"content":"hi"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`
	info, ok := inspectJSON(kindChat, []byte(good))
	if !ok || !info.plainText || info.text != "hi" || info.promptTokens != 2 {
		t.Fatalf("%+v %v", info, ok)
	}
	for name, body := range map[string]string{
		"filtered":   `{"choices":[{"finish_reason":"content_filter","message":{"content":"x"}}]}`,
		"unfinished": `{"choices":[{"finish_reason":null,"message":{"content":"x"}}]}`,
		"error":      `{"error":{"message":"no"}}`,
		"empty":      `{"choices":[]}`,
		"junk":       `<html>`,
	} {
		if _, ok := inspectJSON(kindChat, []byte(body)); ok {
			t.Errorf("%s was accepted", name)
		}
	}
	tools := `{"choices":[{"finish_reason":"tool_calls","message":{"content":null,"tool_calls":[{"id":"1"}]}}]}`
	info, ok = inspectJSON(kindChat, []byte(tools))
	if !ok || info.plainText {
		t.Fatalf("tool calls are cacheable but not plain text: %+v %v", info, ok)
	}
}

func TestParseStream(t *testing.T) {
	s := parseStream([]byte("data: {\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"llo\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2},\"choices\":[]}\n\n" +
		"data: [DONE]\n\n"))
	if !s.done || !s.finished() || s.content.String() != "hello" || s.promptTokens != 4 || s.model != "m" {
		t.Fatalf("%+v", s)
	}
	bad := parseStream([]byte("data: {\"error\":{\"message\":\"x\"}}\n\ndata: [DONE]\n\n"))
	if bad.finished() {
		t.Fatal("a stream carrying an error was accepted")
	}
}

func TestTenantIDCoversOtherCredentialHeadersAndRepeatedValues(t *testing.T) {
	mk := func(name, value string) *http.Request {
		r, _ := http.NewRequest("POST", "http://x", nil)
		r.Header.Set(name, value)
		return r
	}
	if tenantID(mk("X-Goog-Api-Key", "a"), ScopeKey, nil) == tenantID(mk("X-Goog-Api-Key", "b"), ScopeKey, nil) {
		t.Fatal("two callers of the Google header share a tenant")
	}
	if tenantID(mk("X-Custom-Auth", "a"), ScopeKey, nil) != "anonymous" {
		t.Fatal("an unknown header should not count unless it is configured")
	}
	extra := []string{"X-Custom-Auth"}
	if tenantID(mk("X-Custom-Auth", "a"), ScopeKey, extra) == tenantID(mk("X-Custom-Auth", "b"), ScopeKey, extra) {
		t.Fatal("a configured tenant header was ignored")
	}
	r1, _ := http.NewRequest("POST", "http://x", nil)
	r1.Header.Add("Authorization", "Bearer same")
	r1.Header.Add("Authorization", "Bearer first")
	r2, _ := http.NewRequest("POST", "http://x", nil)
	r2.Header.Add("Authorization", "Bearer same")
	r2.Header.Add("Authorization", "Bearer second")
	if tenantID(r1, ScopeKey, nil) == tenantID(r2, ScopeKey, nil) {
		t.Fatal("only the first value of a repeated header was used")
	}
}

func TestBodiesWithTrailingDataAreNotParsed(t *testing.T) {
	if _, err := parseBody([]byte(`{"a":1}{"b":2}`)); err == nil {
		t.Fatal("two JSON objects were accepted as one request")
	}
	if _, err := parseBody([]byte(`{"a":1}   `)); err != nil {
		t.Fatalf("trailing whitespace is fine: %v", err)
	}
}
