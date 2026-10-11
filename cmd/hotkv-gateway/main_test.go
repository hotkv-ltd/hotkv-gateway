// Copyright (c) 2026 HotKV Ltd
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root.

package main

import (
	"strings"
	"testing"
)

func TestVersionAndHelpSucceed(t *testing.T) {
	for _, arg := range []string{"--version", "--help"} {
		if err := run([]string{arg}); err != nil {
			t.Errorf("%s: %v", arg, err)
		}
	}
}

func TestBadSettingsAreRefusedBeforeConnectingAnywhere(t *testing.T) {
	for name, args := range map[string][]string{
		"scope":       {"--scope", "everyone"},
		"upstream":    {"--upstream", "ftp://example.com"},
		"no upstream": {"--upstream", "api.openai.com"},
		"ttl":         {"--ttl", "0s"},
		"flag":        {"--no-such-flag"},
		"log level":   {"--log-level", "shouting"},
		"threshold":   {"--semantic", "--semantic-threshold", "2"},
		"in flight":   {"--max-in-flight", "0"},
	} {
		if err := run(append([]string{"--hotkv-url", "hotkv://127.0.0.1:1"}, args...)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "cannot use the HotKV server") {
			t.Errorf("%s: it tried to connect before checking the settings: %v", name, err)
		}
	}
}

func TestAnUnreachableHotKVServerIsAClearErrorWithoutThePassword(t *testing.T) {
	err := run([]string{"--hotkv-url", "hotkv://user:hunter2@127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "cannot use the HotKV server") {
		t.Fatalf("error: %v", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("the password leaked into the error: %v", err)
	}
}

func TestRedact(t *testing.T) {
	if got := redact("hotkv://u:secret@h:1"); strings.Contains(got, "secret") {
		t.Fatal(got)
	}
	if got := redact("hotkv://h:1"); got != "hotkv://h:1" {
		t.Fatal(got)
	}
	if got := redact("::not a url"); got != "::not a url" {
		t.Fatal(got)
	}
}
