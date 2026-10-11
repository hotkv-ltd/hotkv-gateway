# HotKV Gateway

[![CI](https://github.com/hotkv-ltd/hotkv-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/hotkv-ltd/hotkv-gateway/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A drop-in caching proxy for OpenAI-compatible APIs, backed by [HotKV](https://hotkv.com), a
RESP2/RESP3-compatible in-memory data store with built-in AI and LLM-serving features (semantic and
prompt caching, agent memory, RAG, feature store, rate limiting, time series and more).
Point your app's `base_url` at the gateway and repeated requests are answered from HotKV instead of
being paid for again. No code changes, in any language.

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1")   # the only change
```

- **Exact cache.** An identical chat, completion or embeddings request is answered from the cache.
  Streaming requests are captured and replayed as a stream.
- **Semantic cache (optional).** A differently worded question can be answered by a cached answer to a
  similar one (`--semantic`).
- **Safe by default.** Each API key has its own cache, errors and filtered answers are never cached,
  and requests with tool calls, several choices or high temperature can be left out.
- **Never in the way.** If HotKV is slow or down, requests go straight to the LLM API.
- **Identical requests share one upstream call** while it is in flight.
- One static binary (about 6 MB) or a small container. Works with OpenAI, Azure OpenAI, OpenRouter,
  vLLM, Ollama, LiteLLM and anything else that speaks the OpenAI API.

Written against HotKV server 0.3.0. Tested with the official OpenAI Python SDK.

## Run it

```bash
# a HotKV server on localhost:6379
docker run -p 8080:8080 \
  -e HOTKV_URL=hotkv://host.docker.internal:6379 \
  ghcr.io/hotkv-ltd/hotkv-gateway --upstream https://api.openai.com
```

or build the binary:

```bash
go install github.com/hotkv-ltd/hotkv-gateway/cmd/hotkv-gateway@latest
hotkv-gateway --upstream https://api.openai.com --hotkv-url hotkv://localhost:6379
```

The caller's own `Authorization` header goes to the LLM API, so the gateway needs no key. To keep the
key in the gateway instead, set `HOTKV_GATEWAY_UPSTREAM_API_KEY`; callers then need none, and they
all share one cache.

Every cacheable response carries `X-Hotkv-Cache`: `HIT`, `SEMANTIC-HIT`, `MISS`, `COALESCED` or
`BYPASS`.

## What is cached

| Request | Cached |
| --- | --- |
| `…/chat/completions`, `…/completions`, `…/embeddings` (POST) | yes |
| streaming (`"stream": true`) | yes, kept as the event stream and replayed |
| `n` greater than 1 | no |
| `temperature` above `--max-temperature` (off by default) | no |
| a model outside `--models` (all by default) | no |
| any other path (`/v1/models`, files, images, audio...) | passed through untouched |

A response is stored only when it is a `200` that completed normally: not an error, not
`content_filter`, and for a stream only if it ended with `[DONE]`. The key covers the endpoint, the
query string, the caller's credentials and every body field that can change the answer; `user` and
`metadata` do not. The credentials are the `Authorization`, `Proxy-Authorization`, `X-Api-Key`,
`Api-Key`, `X-Goog-Api-Key`, `OpenAI-Organization`, `OpenAI-Project` and `Cookie` headers. If your
upstream authenticates on another header, name it with `--tenant-headers`, or callers that differ only
in that header would share answers. Requests that differ only in key order or number formatting share an entry.

Per request, a header controls the cache: `X-Hotkv-Cache-Control: no-cache` skips the lookup and
refreshes the entry, `no-store` reads but does not write, `bypass` skips both. The standard
`Cache-Control` header works the same way.

## Semantic cache

```bash
hotkv-gateway --upstream https://api.openai.com --semantic \
  --embedding-model text-embedding-3-small --semantic-threshold 0.92
```

The gateway embeds the last user message with the upstream's embeddings endpoint (using the caller's
key; the embedding itself is cached) and asks HotKV for the nearest cached prompt. It needs a HotKV
license that includes the semantic cache, and the gateway refuses to start with `--semantic` without
one.

A match by meaning is only safe when everything else that shapes the answer is the same, so:

- each system prompt and parameter set gets its own cache namespace;
- conversations with earlier user or assistant turns, and requests with tools or functions, never use
  the semantic cache (they still use the exact cache);
- a hit returns the cached answer text as a new response, with zero token counts in `usage`.

Start with a high threshold and lower it while watching the answers.

If the embeddings endpoint fails or takes longer than `--embedding-timeout` (5 seconds), the semantic
cache pauses for 30 seconds and requests go on without it, so a broken embeddings endpoint never slows
chat requests down. Azure OpenAI addresses embeddings through a separate deployment, so use the exact
cache there. A semantic namespace is created the first time an answer is stored in it, and at most
1000 are created; callers who vary the system prompt or parameters endlessly use them up.

## HotKV licensing

The gateway works on any HotKV server. With a license that includes the prompt cache, entries live in
a `PCACHE` namespace and HotKV tracks tokens saved (`PCACHE.STATS`). Without one it caches under plain
keys and says so at startup. The semantic cache always needs a license.

## Options

| Option | Environment | Default |
| --- | --- | --- |
| `--upstream URL` | `HOTKV_GATEWAY_UPSTREAM` | `https://api.openai.com` |
| `--listen ADDR` | `HOTKV_GATEWAY_LISTEN` | `127.0.0.1:8080` (the container listens on all addresses) |
| `--hotkv-url URL` | `HOTKV_URL` | `hotkv://127.0.0.1:6379`; `hotkvs://` for TLS |
| | `HOTKV_GATEWAY_UPSTREAM_API_KEY` | none |
| `--ttl` | | `1h` |
| `--scope key\|global` | | `key`: a cache per credential. `global` shares answers between callers |
| `--max-temperature N` | | off |
| `--models a,b` | | all |
| `--max-entry-bytes` | | 1 MiB |
| `--max-request-bytes` | | 32 MiB |
| `--cache-timeout` | | `250ms`: how long a cache read or write may take before the request goes on without it |
| `--upstream-timeout` | | `10m` |
| `--stream-idle-timeout` | | `5m`: a stream that sends nothing for this long is cut |
| `--tenant-headers a,b` | | none: extra headers that identify the caller (see above) |
| `--max-in-flight N` | | `1024`: requests handled at once; more get a 503 with `Retry-After` |
| `--embedding-timeout` | | `5s` |
| `--semantic`, `--embedding-model`, `--semantic-threshold` | | off, `text-embedding-3-small`, `0.92` |
| `--prefix` | | `gw`: key and namespace prefix on the HotKV server |
| `--log-level` | | `info` |

## Operating it

- `GET /_hotkv/health` returns the cache mode and version. `GET /_hotkv/metrics` returns Prometheus
  text: requests by result, coalesced requests, stored entries, upstream and cache errors, and tokens
  saved by exact hits.
- The gateway does not authenticate callers or log bodies, and it passes credentials through.
  Run it on a private network or behind an authenticating proxy, and do not expose `/_hotkv/` publicly.
- With `--scope global`, or with a held upstream key, callers share answers. Use that only for
  callers that are allowed to see each other's prompts.
- A stream the upstream breaks halfway is aborted on the caller's side (their connection errors) and
  is never stored. A stream whose caller hangs up cancels the upstream call, so unused tokens are
  not generated.
- A cached answer is served to the key that produced it for the whole `--ttl`, even if that key was
  revoked in the meantime, because the gateway does not validate keys.
- Paths containing `..` are refused. Request bodies are held in memory, up to `--max-request-bytes`
  each and `--max-in-flight` at a time.
- It starts only if the HotKV server is reachable. Afterwards a failing cache never fails a request;
  it is counted in `hotkv_gateway_cache_errors_total`.
- Cached answers have the model's wording at the time they were stored. Lower `--ttl`, or send
  `no-cache`, when answers must follow a model or prompt change.

## Development

```bash
go vet ./... && go test -race ./...                    # no server needed
HOTKV_TEST_URL=hotkv://127.0.0.1:6379 go test -race ./...   # also the live tests
```

The live tests use a random prefix and remove what they create. They pass against a licensed server
(prompt cache and semantic cache) and an unlicensed one (plain keys).

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Support

- Bugs and feature requests: [GitHub issues](https://github.com/hotkv-ltd/hotkv-gateway/issues)
- Questions and commercial support: support@hotkv.com
- Security vulnerabilities: see [SECURITY.md](SECURITY.md)

## License

Licensed under the Apache License, Version 2.0: see [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Trademarks and affiliation

"HotKV" is a trademark of HotKV Ltd; the license does not grant rights to use it.
The HotKV server is a separate commercial product.

HotKV is an independent product of HotKV Ltd. It speaks the RESP protocol and implements
many Redis commands so that existing tools and client habits carry over, but it is not Redis,
Valkey, Dragonfly or KeyDB, and this software is built for and tested against HotKV. HotKV Ltd is not
affiliated with, endorsed by or sponsored by Redis Ltd., the Valkey project, DragonflyDB or KeyDB.
Redis is a registered trademark of Redis Ltd. Valkey, Dragonfly, KeyDB and all other product and
company names are trademarks of their respective owners; they are used here only to describe
protocol and command compatibility. OpenAI is a trademark of OpenAI; this gateway is an independent
project that speaks the publicly documented API and is not affiliated with or endorsed by OpenAI.
