# Direct LLM Translation Package Design

## Goal

Create a small Go package inside Corpscout that translates source registry terms by calling the local LLM directly over an OpenAI-compatible HTTP API.

The package should capture the current benchmark result: sequential requests with batch size 16 are the best known operating point. Sources should use one active LLM request at a time unless a later benchmark proves that parallel requests improve throughput and reliability.

## Context

Corpscout currently has source-specific translation queues and a JetStream buffer design. That queue layer decides which company terms need translation, tracks Postgres batch state, and applies results back into source tables.

The direct Go benchmark showed that calling the local LLM directly is much faster than the Python translation service path. The package should move that proven direct-call logic into Corpscout without importing benchmark-only code and without adding source-specific behavior.

## Package Location

Add the package under:

```text
corpscout/scheduler/internal/directllmtranslation
```

The package is internal to the scheduler because it depends on Corpscout operational conventions and is not intended as a public module.

## API Decision

Use a concrete client with a request/response method as the primary API:

```go
client := directllmtranslation.NewClient(directllmtranslation.Config{
    BaseURL: "http://100.77.62.33:8888",
    Model:   "qwen3:6b",
})

result, err := client.TranslateTerms(ctx, directllmtranslation.TranslateTermsRequest{
    SourceLang: "et",
    TargetLang: "en",
    Terms:      terms,
})
```

Do not expose unbuffered channels as the primary package interface.

Reasons:

- Source actions and queue workers already operate on explicit batches.
- Temporal activities, HTTP handlers, and background services need direct error returns.
- `context.Context` gives clearer cancellation and timeout behavior than channel shutdown protocols.
- Retries and Postgres state transitions need per-batch results, not a long-running stream contract.

Channels can be added later as a thin worker adapter if a long-lived process needs one, but that adapter should call `TranslateTerms` internally.

## Public Types

The package should expose source-neutral structs:

```go
type Config struct {
    BaseURL               string
    APIKey                string
    Model                 string
    RequestTimeout        time.Duration
    MaxBatchSize          int
    MaxConcurrentRequests int
}

type Term struct {
    ID       string
    Text     string
    Category string
}

type TranslateTermsRequest struct {
    SourceLang string
    TargetLang string
    Terms      []Term
}

type TranslateTermsResult struct {
    Translations map[string]string
    RawContent   string
    PromptTokens int
    OutputTokens int
}
```

`ID` is the caller-owned stable identifier. Queue callers can use the translation term key. Benchmark callers can use fixture ids. The package should preserve ids exactly and reject duplicate ids in one request.

Do not introduce a Go interface for the client in the package. The concrete `Client` is the stable API. Tests should use `httptest.Server` at the HTTP boundary instead of mocks.

## Defaults

Default values:

- `MaxBatchSize = 16`
- `MaxConcurrentRequests = 1`
- `RequestTimeout = 300s`
- `MaxTokens = min(max(term_count * 96, 512), 4096)`
- `temperature = 0`
- thinking disabled in request payload when the OpenAI-compatible server supports `chat_template_kwargs.enable_thinking`

`MaxConcurrentRequests` exists so the package can enforce one active request per shared client. App wiring must construct one shared client instance for translation queue workers that target the same local LLM. Creating multiple clients can still create parallel LLM requests, so the README must call this out.

## Prompt Shape

Use the benchmark prompt shape:

```text
/no_think
Translate <source_lang> business registry text to <target_lang>.
Use each item's category as context.
Return only JSON: {"translations":[{"id":"...","translation":"..."}]}
Preserve every input id exactly. Include one translation per input item.
Items: [...]
```

Each compact item contains:

- `id`
- `text`
- `category`

The package should not know source table names, company ids, or `_en` columns. Callers provide category labels that help the model translate registry terms correctly.

## Batch Handling

`TranslateTerms` validates:

- source language is set
- target language is set
- model is set
- base URL is set
- at least one term is present
- term ids are unique and non-empty
- term text is non-empty
- term count does not exceed `MaxBatchSize`

The package should not automatically split oversized input in the first version. Splitting belongs in the queue dispatcher or caller because callers own batch ids, retry state, and Postgres updates.

## Concurrency

The `Client` should enforce `MaxConcurrentRequests` with an internal semaphore. The default and recommended value is one.

This means Brreg and Ariregister can share one direct client and safely submit work from separate goroutines while still producing one active LLM HTTP request at a time.

The package should not attempt cross-process locking. If there are multiple scheduler processes, the global concurrency guarantee must come from deployment configuration or a later distributed limiter.

## Response Parsing

The parser should accept the formats already seen in benchmark work:

- `{"translations":[{"id":"one","translation":"Programming"}]}`
- `{"items":[{"id":"one","translated_text":"Programming"}]}`
- `{"one":"Programming"}`
- a top-level JSON array of item objects
- JSON wrapped in a Markdown code block

Only translations for expected ids should be returned. Unknown ids should be ignored. Missing expected ids are reported by absence from `Translations`; the caller decides whether that is a partial failure.

## Error Handling And Logging

Lower-level package code should wrap and return errors using `github.com/cockroachdb/errors`.

The package should not log normal request failures itself. Queue workers, Temporal activities, or other boundary layers should log once with source, batch id, model, and term count. The package may expose enough structured result data for boundary logs, but it must not log API keys or full sensitive request bodies.

HTTP non-2xx responses should include status code and a small bounded response preview in the wrapped error. The preview must be size-limited.

## README

Add `README.md` in the package directory. It should explain:

- batch size 16 is the current recommended value
- use one shared client instance per local LLM
- keep `MaxConcurrentRequests` at one by default
- sources should pass only one already-sized batch to `TranslateTerms`
- callers own queue state, retries, and applying translations
- channels are intentionally not the primary API

## Integration With Source Queue

The package does not replace Postgres queues or JetStream by itself.

Queue integration should be a separate step:

1. Dispatcher claims a source batch from Postgres.
2. Worker converts queue terms to `directllmtranslation.Term`.
3. Worker calls `TranslateTerms`.
4. Worker converts returned translations to source queue results.
5. Existing source store saves terms and applies bindings.

This keeps direct LLM translation reusable while leaving Brreg and Ariregister data rules in their source packages.

## Testing

Package tests should cover:

- base URL normalization
- prompt contains language pair, JSON-only instruction, and all ids
- validation rejects missing language, duplicate ids, blank text, and oversized batches
- client posts to `/v1/chat/completions`
- client sends model, temperature zero, max tokens, and thinking-disabled payload
- parser reads all supported response shapes
- unknown ids are ignored
- HTTP non-2xx returns a wrapped error
- default concurrency serializes concurrent calls through one client

Default package unit tests should use `httptest.Server` for the HTTP boundary. No NATS, Temporal, Postgres, or real LLM is required for the default test suite.

## Real Local LLM Integration Test

The package should also include an opt-in integration test against the same local LLM style used by the direct Go performance benchmark.

This test should be skipped by default and enabled only with an explicit environment variable so CI and regular local test runs remain deterministic:

```bash
CORPSCOUT_DIRECT_LLM_REAL_TEST=1 \
CORPSCOUT_DIRECT_LLM_BASE_URL=http://100.77.62.33:8888 \
CORPSCOUT_DIRECT_LLM_MODEL=qwen3:6b \
go test ./internal/directllmtranslation -run TestRealLocalLLM -count=1 -v
```

The test should:

- create one shared `Client`
- use `MaxBatchSize = 16`
- use `MaxConcurrentRequests = 1`
- submit exactly one batch of 16 registry-style terms
- include source and target languages from the fixture input, not command-line flags
- verify every input id receives a non-empty translation
- verify no unexpected ids are returned
- print elapsed time and terms per second through `t.Logf`

The fixture should live in the package test code or a small package-local fixture file. It should not import benchmark-only packages from `data-pipelines/golang-translate`.

This real test is not a replacement for the benchmark app. Its purpose is to prove the package contract works against the actual local LLM endpoint and to catch prompt or response-shape regressions before wiring the package into source translation workers.

## Non-Goals

- Replacing the Postgres translation queue.
- Replacing JetStream buffering.
- Implementing Brreg or Ariregister application logic.
- Adding a public channel-based translation API.
- Adding generic provider abstractions beyond OpenAI-compatible chat completions.
- Automatically discovering the best batch size at runtime.
