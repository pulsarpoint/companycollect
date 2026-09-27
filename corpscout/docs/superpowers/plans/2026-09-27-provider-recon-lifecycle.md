# provider_recon evidence lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evidence is never deleted the moment it disappears. Every item carries `active → missing → removed` with dates and a removal action, and there is a grace period per feed and per-collector shape checks. `provider-recon restore` undoes wrong removals. The change manifest gains lifecycle transitions, per-feed churn and run scope, so the backoffice can flag unusual updates. Nothing blocks on a threshold.

**Architecture:**
- `model` gains an embedded `Lifecycle` on every evidence item. `LastSeen` is excluded from the content hash.
- `feeds` gains shape checks (`ErrShape`).
- `definitions` gains per-feed `removal_grace_days` and duplicate-feed rejection.
- `assemble.Build` is rewritten to reconcile the previous document with this run's observations. It adds `assemble.Restore`.
- `publish` diffs lifecycle transitions and writes `scope` and `feeds` into the manifest.
- The CLI gains `restore`.

**Tech Stack:** Go 1.25, stdlib `testing` + `httptest` (as slice 1). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`, sections "Evidence lifecycle and removal" (including "No blocking gate"), "Collector shape checks" and "Change manifest additions".

## Global Constraints

- Module `provider_recon` at `corpscout/services/provider_recon`.
  - Run `go`/`make` commands from there.
  - Run `git` commands from the `corpscout` root and commit by explicit path only, never `git add -A`.
  - Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Status values are exactly `active`, `missing`, `removed`. Removal actions are exactly `grace_expired`, `definition_removed`. Lifecycle dates are UTC days, `2006-01-02`. `restored_at` marks a range put back by `restore`.
- Defaults: `removal_grace_days` 7, and the Azure feed sets 14. Removed items are retained in `latest.json` for 90 days (`RemovedRetentionDays`).
- **No blocking threshold.** A large drop is never held; it follows the grace period like any removal and is visible in the manifest's per-feed churn.
- `restore` only restores `grace_expired` removals. `definition_removed` is never restored, because the definition is the source of truth.
- Only a successful fetch marks items missing, and expiry only runs on a successful fetch. A failed fetch carries every item unchanged (`stale`).
- A removed item that reappears starts a new instance (new `first_seen`). The removed instance is kept.
- Collector statuses are `ok | stale | failed`. `stale` and `failed` are collector issues, and `collect` exits 2 when there are any.
- Run id format: `<YYYYMMDDTHHMMSSZ>-<command>`. History keys use the run id.
- No PulsarProtect imports. Network tests stay behind `-tags live`; S3 tests behind `PROVIDER_RECON_S3_IT=1`.

## Review Focus

1. **A feed that fails for longer than the grace period and then recovers still lacking a range.** The range was missing before the outage. Expected: nothing expires during the outage; the first successful run afterwards applies grace on calendar days since `missing_since`. *(Task 4: `TestLifecycleNoExpiryWhileFeedFails`.)*
2. **The same CIDR removed, then re-added, then removed again within the 90-day retention.** Expected: separate instances, each with its own interval; no merging, no duplicate identity collisions in the diff. *(Task 4: `TestLifecycleReaddAfterRemovalStartsNewInstance`; Task 5: diff identity includes `first_seen`.)*
3. **`restore` with nothing to restore, a bad date, or a range that was already re-added as a new instance.** Expected: an error for nothing/bad date and nothing published; a range that already has a live successor is left alone. `restore` right after a `collect` in the same second must not overwrite that run's manifest. *(Task 4: `TestRestoreRespectsSinceReasonAndSuccessors`, `TestRestoreErrors`; Task 6: run ids carry the command.)*
4. **A document published by slice 1 (items without lifecycle fields).** Expected: upgraded to `active` since that run's day, with no crash and no spurious removals. *(Task 4: `TestLegacyPreviousDocumentIsUpgraded`.)*
5. **Two feeds in one provider publishing the same CIDR for the same service** (Google goog + cloud). Expected: both instances kept (one per collector); one failing leaves the other untouched. *(Task 4: `TestOverlappingFeedsKeepOneInstancePerCollector`.)*

---

## File Structure

```
internal/model/document.go        + Lifecycle (with RestoredAt), statuses/actions, DateLayout, Churn, Service.RemovedAt, CollectorStatus.Churn
internal/feeds/feeds.go           + ErrShape, shapeErr
internal/feeds/{aws,google,cloudflare,fastly,bunny,azure,oracle,github}.go   shape checks
internal/feeds/shape_test.go      new
internal/definitions/types.go     + RemovalGraceDays, Grace(), default
internal/definitions/validate.go  + range check, duplicate feed ids
internal/assemble/assemble.go     rewritten: reconciliation, retention, upgrade, Restore
internal/assemble/lifecycle_test.go   new
internal/publish/diff.go          rewritten: lifecycle transitions (incl. restored)
internal/publish/publish.go       + Scope, FeedRun, RunID, PublishScoped; history keyed by run id
cmd/provider-recon/main.go        + restore command, scoped collect
definitions/microsoft.yaml        azure removal_grace_days: 14
README.md                         lifecycle section
```

---

### Task 1: Lifecycle in the model

**Files:**
- Modify: `internal/model/document.go`
- Test: `internal/model/document_test.go` (append)

**Interfaces:**
- Produces:
  - Constants `StatusActive`, `StatusMissing`, `StatusRemoved`; `ActionGraceExpired`, `ActionDefinitionRemoved`; `DateLayout = "2006-01-02"`
  - `type Lifecycle struct{ Status, FirstSeen, LastSeen, MissingSince, RemovedAt, RemovalAction, RestoredAt string }`, embedded (no field name) in `IPRange`, `ASN`, `DNSRule`, `HTTPRule`, `PTRRule`, `CertificateIdentity`
  - `Service.RemovedAt string`
  - `type Churn struct{ Added, Reappeared, Missing, Removed, Purged int }`
  - `CollectorStatus` addition: `Churn Churn`
  - `ContentHash` ignores `Lifecycle.LastSeen`

- [ ] **Step 1: Write the failing test** (append to `internal/model/document_test.go`)

```go
func TestContentHashIgnoresLastSeenButNotStatus(t *testing.T) {
	d := sampleDoc()
	for i := range d.Services[1].Evidence.IPRanges {
		d.Services[1].Evidence.IPRanges[i].Lifecycle = Lifecycle{Status: StatusActive, FirstSeen: "2026-09-27", LastSeen: "2026-09-27"}
	}
	base, err := ContentHash(d)
	if err != nil {
		t.Fatal(err)
	}
	d.Services[1].Evidence.IPRanges[0].LastSeen = "2026-10-15"
	if got, _ := ContentHash(d); got != base {
		t.Fatal("hash changed when only last_seen advanced")
	}
	d.Services[1].Evidence.IPRanges[0].Status = StatusMissing
	d.Services[1].Evidence.IPRanges[0].MissingSince = "2026-10-15"
	if got, _ := ContentHash(d); got == base {
		t.Fatal("hash must change when an item goes missing")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/model/`
Expected: FAIL, `undefined: Lifecycle`.

- [ ] **Step 3: Implement** in `internal/model/document.go`

Insert after the `Provenance` type and its `hashView` method:
```go
// Item statuses.
const (
	StatusActive  = "active"
	StatusMissing = "missing"
	StatusRemoved = "removed"
)

// Removal actions.
const (
	ActionGraceExpired      = "grace_expired"
	ActionDefinitionRemoved = "definition_removed"
)

// DateLayout is the day resolution of lifecycle dates (UTC).
const DateLayout = "2006-01-02"

// Lifecycle is an evidence item's observed life. An item that reappears after
// being removed is a new instance with its own FirstSeen.
type Lifecycle struct {
	Status        string `json:"status"`
	FirstSeen     string `json:"first_seen"`
	LastSeen      string `json:"last_seen"`
	MissingSince  string `json:"missing_since,omitempty"`
	RemovedAt     string `json:"removed_at,omitempty"`
	RemovalAction string `json:"removal_action,omitempty"`
	// RestoredAt records that restore undid a wrong removal of this instance.
	RestoredAt string `json:"restored_at,omitempty"`
}

// hashView drops LastSeen: it advances every day without the evidence changing.
func (l Lifecycle) hashView() Lifecycle {
	l.LastSeen = ""
	return l
}
```

Add `Lifecycle` as an embedded field directly after `Provenance` in each of `IPRange`, `ASN`, `DNSRule`, `HTTPRule`, `PTRRule` and `CertificateIdentity`, for example:
```go
	Note       string  `json:"note,omitempty"`
	Provenance
	Lifecycle
}
```

Add to `Service` after `Traits`:
```go
	// RemovedAt is set when the service was dropped from the definition; the
	// service stays until its removed items are purged.
	RemovedAt string   `json:"removed_at,omitempty"`
```

Add the churn type and extend `CollectorStatus`:
```go
// Churn counts one run's lifecycle transitions for one feed.
type Churn struct {
	Added      int `json:"added"`
	Reappeared int `json:"reappeared"`
	Missing    int `json:"missing"`
	Removed    int `json:"removed"`
	Purged     int `json:"purged"`
}
```
In `CollectorStatus`, append:
```go
	Churn Churn `json:"churn"`
```

In `ContentHash`, next to each `...Provenance = ...Provenance.hashView()` line, add the matching lifecycle line. For example, for IP ranges:
```go
		for j := range e.IPRanges {
			e.IPRanges[j].Provenance = e.IPRanges[j].Provenance.hashView()
			e.IPRanges[j].Lifecycle = e.IPRanges[j].Lifecycle.hashView()
		}
```
Do the same for `ASNs`, `DNSRules`, `HTTPRules`, `PTRRules` and `CertificateIdentities`. Update the `ContentHash` doc comment to also name "the lifecycle's last_seen".

- [ ] **Step 4: Run to verify pass**

Run: `go test ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS. Every existing package still passes, because a zero `Lifecycle` marshals as `"status":""`, which is harmless.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/model
git commit -m "feat(provider_recon): evidence lifecycle fields in the v1 model"
```

---

### Task 2: Collector shape checks

**Files:**
- Modify: `internal/feeds/feeds.go`, `aws.go`, `google.go`, `cloudflare.go`, `fastly.go`, `bunny.go`, `azure.go`, `oracle.go`, `github.go`
- Modify tests: `internal/feeds/aws_test.go` (`TestAWSEmptyFeedIsAnError`), `internal/feeds/cloud_test.go` (Azure fixture)
- Test: `internal/feeds/shape_test.go` (new)

**Interfaces:**
- Produces: `var ErrShape = errors.New("feed shape changed")` and `func shapeErr(format string, args ...any) error` (wraps `ErrShape`).

- [ ] **Step 1: Write the failing tests** — `internal/feeds/shape_test.go`

```go
package feeds

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestShapeChecks(t *testing.T) {
	cases := []struct {
		name string
		body string
		make func(url string) Collector
	}{
		{"aws without ipv6", `{"syncToken":"1","prefixes":[{"ip_prefix":"1.0.0.0/24","region":"x","service":"EC2"}],"ipv6_prefixes":[]}`,
			func(u string) Collector { return &AWS{URL: u} }},
		{"google cloud without ipv6", `{"syncToken":"1","prefixes":[{"ipv4Prefix":"1.0.0.0/24","service":"Google Cloud","scope":"x"}]}`,
			func(u string) Collector { return &GoogleCloud{URL: u} }},
		{"cloudflare without ipv6", `{"success":true,"result":{"ipv4_cidrs":["1.0.0.0/24"],"ipv6_cidrs":[],"etag":"e"}}`,
			func(u string) Collector { return &Cloudflare{URL: u} }},
		{"fastly without ipv6", `{"addresses":["1.0.0.0/24"],"ipv6_addresses":[]}`,
			func(u string) Collector { return &Fastly{URL: u} }},
		{"oracle without OCI", `{"last_updated_timestamp":"t","regions":[{"region":"r","cidrs":[{"cidr":"1.0.0.0/24","tags":["OSN"]}]}]}`,
			func(u string) Collector { return &Oracle{URL: u} }},
		{"github without pages", `{"hooks":["1.0.0.0/24"]}`,
			func(u string) Collector { return &GitHub{URL: u} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := serveBody(c.body)
			defer srv.Close()
			_, err := c.make(srv.URL).Collect(context.Background(), testFetcher(srv), nil)
			if !errors.Is(err, ErrShape) {
				t.Fatalf("err = %v, want ErrShape", err)
			}
		})
	}
}

func TestBunnyEmptyIPv6ListIsAShapeError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`["1.2.3.4"]`)) })
	mux.HandleFunc("/v6", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[]`)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	_, err := (&Bunny{URLv4: srv.URL + "/v4", URLv6: srv.URL + "/v6"}).Collect(context.Background(), testFetcher(srv), nil)
	if !errors.Is(err, ErrShape) {
		t.Fatalf("err = %v, want ErrShape", err)
	}
}

func TestAzureWithoutAzureCloudIsAShapeError(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<a href="%s/download/x/ServiceTags_Public_20260921.json">x</a>`, srv.URL)
	})
	mux.HandleFunc("/download/x/ServiceTags_Public_20260921.json", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"changeNumber":1,"values":[{"name":"AzureFrontDoor.Frontend","properties":{"addressPrefixes":["13.107.246.0/24"]}}]}`))
	})
	c := &Azure{PageURL: srv.URL + "/page",
		LinkPattern: regexp.MustCompile(regexp.QuoteMeta(srv.URL) + `/download/[^"]+/ServiceTags_Public_\d{8}\.json`)}
	_, err := c.Collect(context.Background(), testFetcher(srv), nil)
	if !errors.Is(err, ErrShape) {
		t.Fatalf("err = %v, want ErrShape", err)
	}
}
```

Update the existing tests:
- In `internal/feeds/aws_test.go`, `TestAWSEmptyFeedIsAnError`: replace `errors.Is(err, ErrEmptyFeed)` with `errors.Is(err, ErrShape)` and the message `want ErrEmptyFeed` with `want ErrShape`. An AWS file with both lists empty is now a shape change.
- In `internal/feeds/cloud_test.go`, `TestAzureCollectFollowsDownloadLink`:
  - Add a fourth value to the served JSON, after the `AppService.WestEurope` entry:
    ```
    ,{"name":"AzureCloud","properties":{"region":"","addressPrefixes":["20.50.3.0/24"]}}
    ```
  - Change `len(res.Ranges) != 3` to `len(res.Ranges) != 4`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/feeds/`
Expected: FAIL: `undefined: ErrShape`.

- [ ] **Step 3: Implement**

In `internal/feeds/feeds.go`, add `"fmt"` to the imports and add after `ErrEmptyFeed`:
```go
// ErrShape means a publication decoded but no longer has the structure the
// collector relies on: a format change, never a legitimate shrink. The feed
// goes stale instead of publishing a partial range set.
var ErrShape = errors.New("feed shape changed")

func shapeErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrShape, fmt.Sprintf(format, args...))
}
```

In `aws.go`, directly after the successful `json.Unmarshal`:
```go
	if len(feed.Prefixes) == 0 || len(feed.IPv6Prefixes) == 0 {
		return Result{}, shapeErr("aws: expected prefixes and ipv6_prefixes, got %d and %d", len(feed.Prefixes), len(feed.IPv6Prefixes))
	}
```

In `google.go`, `collectGoogle`: count families while iterating, and check after the loop, before `return finish(b.res)`:
```go
	v4, v6 := 0, 0
	for _, p := range feed.Prefixes {
		tag := fixedTag
		if tag == "" {
			tag = p.Service
		}
		raw := p.IPv4Prefix
		if raw == "" {
			raw = p.IPv6Prefix
			v6++
		} else {
			v4++
		}
		b.add(raw, tag, p.Scope)
	}
	if v4 == 0 || v6 == 0 {
		return Result{}, shapeErr("google: %s has %d IPv4 and %d IPv6 prefixes", url, v4, v6)
	}
	return finish(b.res)
```
(This replaces the existing loop and return.)

In `cloudflare.go`, after the `success` check:
```go
	if len(feed.Result.IPv4) == 0 || len(feed.Result.IPv6) == 0 {
		return Result{}, shapeErr("cloudflare: expected ipv4_cidrs and ipv6_cidrs, got %d and %d", len(feed.Result.IPv4), len(feed.Result.IPv6))
	}
```

In `fastly.go`, after the unmarshal:
```go
	if len(feed.Addresses) == 0 || len(feed.IPv6) == 0 {
		return Result{}, shapeErr("fastly: expected addresses and ipv6_addresses, got %d and %d", len(feed.Addresses), len(feed.IPv6))
	}
```

In `bunny.go`, inside the loop, after the unmarshal:
```go
		if len(ips) == 0 {
			return Result{}, shapeErr("bunny: %s returned an empty list", url)
		}
```

In `azure.go`, after the unmarshal:
```go
	names := map[string]bool{}
	for _, v := range feed.Values {
		names[v.Name] = true
	}
	for _, required := range []string{"AzureCloud", "AzureFrontDoor.Frontend"} {
		if !names[required] {
			return Result{}, shapeErr("azure: service tag %q missing from %s", required, url)
		}
	}
```

In `oracle.go`, track `hasOCI` in the tag loop (`if tag == "OCI" { hasOCI = true }`), and before `return finish(b.res)`:
```go
	if !hasOCI {
		return Result{}, shapeErr("oracle: no range carries the OCI tag")
	}
```

In `github.go`, before `return finish(res)`:
```go
	hasPages := false
	for _, r := range res.Ranges {
		if r.Tag == "pages" {
			hasPages = true
			break
		}
	}
	if !hasPages {
		return Result{}, shapeErr("github: meta has no pages ranges")
	}
```

- [ ] **Step 4: Run to verify pass, then check the real feeds**

Run: `go test ./internal/feeds/ && go vet ./... && test -z "$(gofmt -l .)" && make live`
Expected:
- Unit tests PASS.
- Every live subtest PASS: the real feeds satisfy every shape check.
- If a live feed fails a shape check, the check is wrong for the real publication. Fix the check, not the threshold, and ledger the ruling.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/feeds
git commit -m "feat(provider_recon): collector shape checks turn format changes into stale feeds"
```

---

### Task 3: Feed lifecycle settings in definitions

**Files:**
- Modify: `internal/definitions/types.go`, `internal/definitions/validate.go`, `definitions/schema.json` (regenerated)
- Test: `internal/definitions/definitions_test.go` (append)

**Interfaces:**
- Produces:
  - `FeedRef.RemovalGraceDays int` (yaml `removal_grace_days,omitempty`)
  - `const DefaultRemovalGraceDays = 7`
  - `func (f FeedRef) Grace() int`

- [ ] **Step 1: Write the failing tests** (append to `definitions_test.go`)

```go
func TestFeedGraceDefault(t *testing.T) {
	if g := (FeedRef{}).Grace(); g != 7 {
		t.Fatalf("default grace = %d", g)
	}
	if g := (FeedRef{RemovalGraceDays: 14}).Grace(); g != 14 {
		t.Fatalf("explicit grace = %d", g)
	}
}

func TestValidateFeedLifecycleSettings(t *testing.T) {
	feed := func(mut func(*FeedRef)) Definition {
		d := base()
		f := FeedRef{Collector: "fake_feed", TagMap: map[string]string{"X": "acme.web"}}
		mut(&f)
		d.Feeds = []FeedRef{f}
		return d
	}
	cases := []struct {
		name string
		def  Definition
		want string
	}{
		{"negative grace", feed(func(f *FeedRef) { f.RemovalGraceDays = -1 }), "removal_grace_days: must be between 1 and 365"},
		{"huge grace", feed(func(f *FeedRef) { f.RemovalGraceDays = 400 }), "removal_grace_days: must be between 1 and 365"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate([]Definition{c.def}, collectors())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
		})
	}

	d := base()
	d.Feeds = []FeedRef{
		{Collector: "fake_feed", TagMap: map[string]string{"X": "acme.web"}},
		{Collector: "fake_feed", TagMap: map[string]string{"Y": "acme.web"}},
	}
	err := Validate([]Definition{d}, collectors())
	if err == nil || !strings.Contains(err.Error(), `duplicate feed "fake_feed"`) {
		t.Fatalf("duplicate feed: err = %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/definitions/`
Expected: FAIL: `f.Grace undefined`.

- [ ] **Step 3: Implement**

In `types.go`, add to `FeedRef` after `GenericTags`:
```go
	// RemovalGraceDays is how long a range may be missing from successful
	// fetches before it is marked removed. 0 means DefaultRemovalGraceDays.
	RemovalGraceDays int `yaml:"removal_grace_days,omitempty"`
```
Add, after the `FeedRef` type:
```go
// DefaultRemovalGraceDays applies when a feed sets no removal_grace_days.
const DefaultRemovalGraceDays = 7

// Grace returns the feed's removal grace period in days.
func (f FeedRef) Grace() int {
	if f.RemovalGraceDays > 0 {
		return f.RemovalGraceDays
	}
	return DefaultRemovalGraceDays
}
```

In `validate.go`, `validateFeed`, add before the `GenericTags` loop:
```go
	if f.RemovalGraceDays < 0 || f.RemovalGraceDays > 365 {
		p.add(d, path+".removal_grace_days", "must be between 1 and 365 (omit for the default %d)", DefaultRemovalGraceDays)
	}
```
In `validateProvider`, replace the feed loop with:
```go
	feedIDs := map[string]bool{}
	for i := range d.Feeds {
		path := fmt.Sprintf("feeds[%d]", i)
		validateFeed(d, &d.Feeds[i], path, serviceKeys, collectors, p)
		id := d.Feeds[i].ID()
		if feedIDs[id] {
			p.add(d, path, "duplicate feed %q; merge the tag_maps into one entry", id)
		}
		feedIDs[id] = true
	}
```

- [ ] **Step 4: Regenerate the schema and run**

Run: `go test ./internal/definitions -run TestSchemaIsCurrent -update && go test ./... && go vet ./... && test -z "$(gofmt -l .)" && go run ./cmd/provider-recon validate`
Expected: PASS; `37 definitions valid`; `rg removal_grace_days definitions/schema.json` finds it.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/definitions services/provider_recon/definitions/schema.json
git commit -m "feat(provider_recon): per-feed removal grace period, reject duplicate feeds"
```

---

### Task 4: Reconciling assemble.Build and assemble.Restore

**Files:**
- Replace: `internal/assemble/assemble.go` (full content below)
- Test: `internal/assemble/lifecycle_test.go` (new). The existing `assemble_test.go` must keep passing unchanged.

**Interfaces:**
- Consumes:
  - `model.Lifecycle`, statuses/actions, `DateLayout`, `Churn` (Task 1)
  - `FeedRef.Grace()` (Task 3)
- Produces:
  - `Build(def, outcomes, prev, now)` with the same signature; now reconciles lifecycles.
  - `func Restore(doc model.Document, collectorID, removedSince string, now time.Time) (model.Document, int, error)`
  - `const RemovedRetentionDays = 90`

- [ ] **Step 1: Write the failing tests** — `internal/assemble/lifecycle_test.go`

```go
package assemble

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

func day(n int) time.Time { return t0.AddDate(0, 0, n) }

func dstr(n int) string { return day(n).Format(model.DateLayout) }

func lcDef() definitions.Definition {
	return definitions.Definition{
		Slug: "aws", DisplayName: "AWS", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"}}},
		Feeds:    []definitions.FeedRef{{Collector: "aws_ip_ranges", TagMap: map[string]string{"CLOUDFRONT": "aws.cloudfront"}}},
	}
}

// cidrs returns n distinct CLOUDFRONT ranges, skipping the indexes in drop.
func cidrs(n int, drop ...int) map[string]FeedOutcome {
	skip := map[int]bool{}
	for _, d := range drop {
		skip[d] = true
	}
	var rs []feeds.Range
	for i := 0; i < n; i++ {
		if !skip[i] {
			rs = append(rs, r(fmt.Sprintf("10.0.%d.0/24", i), "CLOUDFRONT", ""))
		}
	}
	return map[string]FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: rs}}}
}

func failed() map[string]FeedOutcome {
	return map[string]FeedOutcome{"aws_ip_ranges": {Err: fmt.Errorf("status 503")}}
}

// run builds day n on top of prev (nil for the first run).
func run(t *testing.T, def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, n int) model.Document {
	t.Helper()
	doc, err := Build(def, outcomes, prev, day(n))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func ranges(doc model.Document, cidr string) []model.IPRange {
	var out []model.IPRange
	for _, s := range doc.Services {
		for _, ip := range s.Evidence.IPRanges {
			if ip.CIDR == cidr {
				out = append(out, ip)
			}
		}
	}
	return out
}

func one(t *testing.T, doc model.Document, cidr string) model.IPRange {
	t.Helper()
	rs := ranges(doc, cidr)
	if len(rs) != 1 {
		t.Fatalf("%s: %d instances, want 1: %+v", cidr, len(rs), rs)
	}
	return rs[0]
}

func TestLifecycleNewRangeIsActiveFromToday(t *testing.T) {
	d := run(t, lcDef(), cidrs(1), nil, 0)
	got := one(t, d, "10.0.0.0/24").Lifecycle
	want := model.Lifecycle{Status: model.StatusActive, FirstSeen: dstr(0), LastSeen: dstr(0)}
	if got != want {
		t.Fatalf("lifecycle = %+v, want %+v", got, want)
	}
	if c := d.Collection.Collectors["aws_ip_ranges"].Churn; c.Added != 1 {
		t.Fatalf("churn = %+v", c)
	}
}

func TestLifecycleCarriedRangeKeepsFirstSeen(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(1), nil, 0)
	d3 := run(t, lcDef(), cidrs(1), &d0, 3)
	got := one(t, d3, "10.0.0.0/24").Lifecycle
	if got.FirstSeen != dstr(0) || got.LastSeen != dstr(3) || got.Status != model.StatusActive {
		t.Fatalf("lifecycle = %+v", got)
	}
	if c := d3.Collection.Collectors["aws_ip_ranges"].Churn; c != (model.Churn{}) {
		t.Fatalf("churn = %+v, want zero", c)
	}
	if d0.Collection.ContentHash != d3.Collection.ContentHash {
		t.Fatal("hash changed although only last_seen advanced")
	}
}

func TestLifecycleMissingThenRemovedAfterGrace(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	m := one(t, d1, "10.0.3.0/24")
	if m.Status != model.StatusMissing || m.MissingSince != dstr(1) || m.LastSeen != dstr(0) {
		t.Fatalf("day1 = %+v", m.Lifecycle)
	}
	st := d1.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "ok" || st.Churn.Missing != 1 || st.Items != 10 {
		t.Fatalf("day1 status = %+v", st)
	}
	d5 := run(t, lcDef(), cidrs(10, 3), &d1, 5)
	if one(t, d5, "10.0.3.0/24").Status != model.StatusMissing {
		t.Fatal("removed before the 7-day grace period ended")
	}
	d8 := run(t, lcDef(), cidrs(10, 3), &d5, 8)
	rm := one(t, d8, "10.0.3.0/24")
	if rm.Status != model.StatusRemoved || rm.RemovedAt != dstr(8) || rm.RemovalAction != model.ActionGraceExpired {
		t.Fatalf("day8 = %+v", rm.Lifecycle)
	}
	if st := d8.Collection.Collectors["aws_ip_ranges"]; st.Churn.Removed != 1 || st.Items != 9 {
		t.Fatalf("day8 status = %+v", st)
	}
}

func TestLifecycleReappearWithinGrace(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	d2 := run(t, lcDef(), cidrs(10), &d1, 2)
	got := one(t, d2, "10.0.3.0/24").Lifecycle
	if got.Status != model.StatusActive || got.FirstSeen != dstr(0) || got.MissingSince != "" || got.LastSeen != dstr(2) {
		t.Fatalf("lifecycle = %+v", got)
	}
	if c := d2.Collection.Collectors["aws_ip_ranges"].Churn; c.Reappeared != 1 {
		t.Fatalf("churn = %+v", c)
	}
}

func TestLifecycleReaddAfterRemovalStartsNewInstance(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 3), &d1, 8)
	d9 := run(t, lcDef(), cidrs(10), &d8, 9)
	rs := ranges(d9, "10.0.3.0/24")
	if len(rs) != 2 {
		t.Fatalf("%d instances, want the removed one plus a new one: %+v", len(rs), rs)
	}
	var removed, active int
	for _, x := range rs {
		switch {
		case x.Status == model.StatusRemoved && x.FirstSeen == dstr(0):
			removed++
		case x.Status == model.StatusActive && x.FirstSeen == dstr(9):
			active++
		}
	}
	if removed != 1 || active != 1 {
		t.Fatalf("instances = %+v", rs)
	}
}

func TestLifecycleNoExpiryWhileFeedFails(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	prev := d1
	for n := 2; n <= 12; n++ {
		prev = run(t, lcDef(), failed(), &prev, n)
		if st := prev.Collection.Collectors["aws_ip_ranges"]; st.Status != "stale" {
			t.Fatalf("day%d status = %q", n, st.Status)
		}
		if one(t, prev, "10.0.3.0/24").Status != model.StatusMissing {
			t.Fatalf("day%d: expired while the feed was failing", n)
		}
		if one(t, prev, "10.0.4.0/24").Status != model.StatusActive {
			t.Fatalf("day%d: a present range changed status while the feed was failing", n)
		}
	}
	d13 := run(t, lcDef(), cidrs(10, 3), &prev, 13)
	if rm := one(t, d13, "10.0.3.0/24"); rm.Status != model.StatusRemoved || rm.RemovalAction != model.ActionGraceExpired {
		t.Fatalf("first successful run after the outage did not expire it: %+v", rm.Lifecycle)
	}
}

func TestLargeDropIsNotBlockedAndFollowsGrace(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d0, 1)
	st := d1.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "ok" || st.Churn.Missing != 5 || st.Items != 10 {
		t.Fatalf("day1 status = %+v", st)
	}
	d8 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d1, 8)
	if st := d8.Collection.Collectors["aws_ip_ranges"]; st.Churn.Removed != 5 || st.Items != 5 {
		t.Fatalf("day8 status = %+v", st)
	}
}

func TestRestoreUndoesWrongRemovals(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 0, 1, 2, 3, 4), &d1, 8)
	restored, n, err := Restore(d8, "aws_ip_ranges", dstr(8), day(8))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("restored %d, want 5", n)
	}
	got := one(t, restored, "10.0.0.0/24").Lifecycle
	want := model.Lifecycle{Status: model.StatusActive, FirstSeen: dstr(0), LastSeen: dstr(0), RestoredAt: dstr(8)}
	if got != want {
		t.Fatalf("restored lifecycle = %+v, want %+v", got, want)
	}
	if st := restored.Collection.Collectors["aws_ip_ranges"]; st.Items != 10 {
		t.Fatalf("items after restore = %d", st.Items)
	}
	if restored.Collection.ContentHash == d8.Collection.ContentHash {
		t.Fatal("restore must change the content hash")
	}
	// The collector is fixed and the feed lists the ranges again: the
	// timeline continues from the original first_seen, nothing is "added".
	d9 := run(t, lcDef(), cidrs(10), &restored, 9)
	if st := d9.Collection.Collectors["aws_ip_ranges"]; st.Churn.Added != 0 || st.Churn.Missing != 0 {
		t.Fatalf("run after restore churn = %+v", st.Churn)
	}
	if l := one(t, d9, "10.0.0.0/24").Lifecycle; l.FirstSeen != dstr(0) || l.LastSeen != dstr(9) || l.RestoredAt != dstr(8) {
		t.Fatalf("lifecycle after restore + collect = %+v", l)
	}
}

func TestRestoreRespectsSinceReasonAndSuccessors(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 0), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 0, 1), &d1, 8) // 10.0.0 removed on day 8, 10.0.1 missing since day 8
	d15 := run(t, lcDef(), cidrs(10, 0, 1), &d8, 15)
	// 10.0.1 was removed on day 15. 10.0.0 came back as a new instance on day 16.
	d16 := run(t, lcDef(), cidrs(10, 1), &d15, 16)

	if _, _, err := Restore(d16, "aws_ip_ranges", dstr(16), day(16)); err == nil {
		t.Fatal("nothing was removed on or after day 16; restore must fail")
	}
	restored, n, err := Restore(d16, "aws_ip_ranges", dstr(8), day(16))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("restored %d, want only 10.0.1 (10.0.0 already has a live successor)", n)
	}
	if l := one(t, restored, "10.0.1.0/24").Lifecycle; l.Status != model.StatusActive || l.RestoredAt != dstr(16) {
		t.Fatalf("10.0.1 = %+v", l)
	}
	for _, x := range ranges(restored, "10.0.0.0/24") {
		if x.FirstSeen == dstr(0) && x.Status != model.StatusRemoved {
			t.Fatalf("the old 10.0.0 instance must stay removed next to its successor: %+v", x.Lifecycle)
		}
	}

	withRule := lcDef()
	withRule.Services[0].DNSRules = []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}
	r0 := run(t, withRule, cidrs(1), nil, 0)
	r1 := run(t, lcDef(), cidrs(1), &r0, 1) // the rule is definition_removed
	if _, _, err := Restore(r1, "aws_ip_ranges", dstr(0), day(1)); err == nil {
		t.Fatal("definition removals are never restored; nothing else was removed, so restore must fail")
	}
}

func TestRestoreErrors(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(3), nil, 0)
	if _, _, err := Restore(d0, "nope", dstr(0), day(0)); err == nil {
		t.Fatal("unknown collector must fail")
	}
	if _, _, err := Restore(d0, "aws_ip_ranges", "2026/09/27", day(0)); err == nil {
		t.Fatal("a malformed date must fail")
	}
	if _, _, err := Restore(d0, "aws_ip_ranges", dstr(0), day(0)); err == nil {
		t.Fatal("a clean feed has nothing to restore")
	}
}

func TestCuratedRuleRemovedFromDefinition(t *testing.T) {
	withRule := lcDef()
	withRule.Services[0].DNSRules = []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}
	d0 := run(t, withRule, cidrs(1), nil, 0)
	d1 := run(t, lcDef(), cidrs(1), &d0, 1)
	rules := d1.Services[0].Evidence.DNSRules
	if len(rules) != 1 || rules[0].Status != model.StatusRemoved || rules[0].RemovalAction != model.ActionDefinitionRemoved || rules[0].RemovedAt != dstr(1) {
		t.Fatalf("rules = %+v", rules)
	}
	d2 := run(t, withRule, cidrs(1), &d1, 2)
	if rules := d2.Services[0].Evidence.DNSRules; len(rules) != 2 {
		t.Fatalf("re-added rule should be a new instance next to the removed one: %+v", rules)
	}
}

func TestServiceRemovedFromDefinitionThenPurged(t *testing.T) {
	two := lcDef()
	two.Services = append(two.Services, definitions.ServiceDef{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}})
	two.Feeds[0].TagMap["EC2"] = "aws.ec2"
	out := map[string]FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: []feeds.Range{
		r("10.0.0.0/24", "CLOUDFRONT", ""), r("3.5.140.0/22", "EC2", ""),
	}}}}
	d0 := run(t, two, out, nil, 0)
	d1 := run(t, lcDef(), out, &d0, 1)
	ec2 := one(t, d1, "3.5.140.0/22")
	if ec2.Status != model.StatusRemoved || ec2.RemovalAction != model.ActionDefinitionRemoved {
		t.Fatalf("ec2 range = %+v", ec2.Lifecycle)
	}
	var svc *model.Service
	for i := range d1.Services {
		if d1.Services[i].Key == "aws.ec2" {
			svc = &d1.Services[i]
		}
	}
	if svc == nil || svc.RemovedAt != dstr(1) {
		t.Fatalf("removed service not kept with removed_at: %+v", svc)
	}
	if tags := d1.Collection.Collectors["aws_ip_ranges"].UnmappedTags; len(tags) != 1 || tags[0] != "EC2" {
		t.Fatalf("unmapped = %v", tags)
	}
	d93 := run(t, lcDef(), out, &d1, 93)
	for _, s := range d93.Services {
		if s.Key == "aws.ec2" {
			t.Fatal("service with only purged items should disappear")
		}
	}
	if len(ranges(d93, "3.5.140.0/22")) != 0 {
		t.Fatal("removed range not purged after 90 days")
	}
}

func TestRetentionPurgesRemovedAfter90Days(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(10), nil, 0)
	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
	d8 := run(t, lcDef(), cidrs(10, 3), &d1, 8)
	d98 := run(t, lcDef(), cidrs(10, 3), &d8, 98)
	if len(ranges(d98, "10.0.3.0/24")) != 1 {
		t.Fatal("purged before the 90-day retention ended")
	}
	d99 := run(t, lcDef(), cidrs(10, 3), &d98, 99)
	if len(ranges(d99, "10.0.3.0/24")) != 0 {
		t.Fatal("not purged after 90 days")
	}
	if c := d99.Collection.Collectors["aws_ip_ranges"].Churn; c.Purged != 1 {
		t.Fatalf("churn = %+v", c)
	}
}

func TestFeedDroppedFromDefinitionRemovesItsRanges(t *testing.T) {
	d0 := run(t, lcDef(), cidrs(2), nil, 0)
	noFeed := lcDef()
	noFeed.Feeds = nil
	d1 := run(t, noFeed, nil, &d0, 1)
	for _, x := range ranges(d1, "10.0.0.0/24") {
		if x.Status != model.StatusRemoved || x.RemovalAction != model.ActionDefinitionRemoved {
			t.Fatalf("range = %+v", x.Lifecycle)
		}
	}
	if _, ok := d1.Collection.Collectors["aws_ip_ranges"]; ok {
		t.Fatal("a dropped feed must not report a status")
	}
}

func TestLegacyPreviousDocumentIsUpgraded(t *testing.T) {
	legacy := model.Document{Version: model.ContractVersion, Slug: "aws",
		Services: []model.Service{{Key: "aws.cloudfront", Evidence: model.Evidence{IPRanges: []model.IPRange{{
			CIDR: "10.0.0.0/24", FeedTag: "CLOUDFRONT", Confidence: 1,
			Provenance: model.Provenance{Source: model.SourceOfficialFeed, Collector: "aws_ip_ranges"},
		}}}}},
		Collection: model.Collection{CollectedAt: day(0), Collectors: map[string]model.CollectorStatus{
			"aws_ip_ranges": {Status: "ok", LastSuccessAt: func() *time.Time { x := day(0); return &x }()},
		}},
	}
	d1 := run(t, lcDef(), cidrs(1), &legacy, 1)
	got := one(t, d1, "10.0.0.0/24").Lifecycle
	if got.Status != model.StatusActive || got.FirstSeen != dstr(0) || got.LastSeen != dstr(1) {
		t.Fatalf("lifecycle = %+v", got)
	}
	if c := d1.Collection.Collectors["aws_ip_ranges"].Churn; c.Added != 0 {
		t.Fatalf("legacy range counted as new: %+v", c)
	}
}

func TestOverlappingFeedsKeepOneInstancePerCollector(t *testing.T) {
	def := definitions.Definition{
		Slug: "google", DisplayName: "Google", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "google.frontend", DisplayName: "Google", ServiceTypes: []string{"hosting"}}},
		Feeds: []definitions.FeedRef{
			{Collector: "google_goog", TagMap: map[string]string{"GOOG": "google.frontend"}},
			{Collector: "google_cloud", TagMap: map[string]string{"Google Cloud": "google.frontend"}},
		},
	}
	both := map[string]FeedOutcome{
		"google_goog":  {Result: feeds.Result{Ranges: []feeds.Range{r("34.0.0.0/15", "GOOG", "")}}},
		"google_cloud": {Result: feeds.Result{Ranges: []feeds.Range{r("34.0.0.0/15", "Google Cloud", "us-east1")}}},
	}
	d0 := run(t, def, both, nil, 0)
	if n := len(ranges(d0, "34.0.0.0/15")); n != 2 {
		t.Fatalf("%d instances, want one per collector", n)
	}
	cloudFails := map[string]FeedOutcome{"google_goog": both["google_goog"], "google_cloud": {Err: fmt.Errorf("timeout")}}
	d1 := run(t, def, cloudFails, &d0, 1)
	rs := ranges(d1, "34.0.0.0/15")
	if len(rs) != 2 {
		t.Fatalf("a failing collector lost its overlapping range: %+v", rs)
	}
	for _, x := range rs {
		if x.Status != model.StatusActive {
			t.Fatalf("range = %+v", x)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/assemble/`
Expected: FAIL (`undefined: Restore`, …).

- [ ] **Step 3: Replace** `internal/assemble/assemble.go`

```go
// Package assemble turns a curated definition plus this run's feed outcomes
// into a provider-recon/v1 document, reconciling every item's lifecycle
// against the previous document.
package assemble

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

// FeedOutcome is one collector run: a result or an error.
type FeedOutcome struct {
	Result feeds.Result
	Err    error
}

const (
	curatedConfidence  = 1.0
	officialConfidence = 1.0
	bgpConfidence      = 0.8
	// RemovedRetentionDays is how long a removed item stays in latest.json.
	// History objects and the ClickHouse timeline keep it forever.
	RemovedRetentionDays = 90
)

// bgpCollectors publish announcements, not operator-published ranges.
var bgpCollectors = map[string]bool{"ripestat_announced": true}

// svcRange is a feed range with the service it belongs to.
type svcRange struct {
	svc  string
	item model.IPRange
}

// Build assembles the document for one run.
//
// Curated evidence follows the definition: an item deleted there is removed
// at once. Feed ranges follow the feed: a range absent from a successful
// fetch goes missing, and is removed once its grace period has passed. A
// failed fetch changes nothing (stale). No threshold blocks a large drop; it
// shows up in the collector's churn, and Restore undoes a wrong removal.
func Build(def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, now time.Time) (model.Document, error) {
	today := now.UTC().Format(model.DateLayout)
	prev = upgrade(prev)
	doc := model.Document{
		Version: model.ContractVersion, Slug: def.Slug, DisplayName: def.DisplayName,
		Category: def.Category, Website: def.Website, Country: def.Country,
		Aliases: def.Aliases, ProviderKeys: def.ProviderKeys,
		Collection: model.Collection{CollectedAt: now, Collectors: map[string]model.CollectorStatus{}},
	}

	index := map[string]int{}
	defined := map[string]bool{}
	curatedBy := map[string]model.Evidence{}
	for _, s := range def.Services {
		defined[s.Key] = true
		curatedBy[s.Key] = curatedEvidence(s)
		index[s.Key] = len(doc.Services)
		doc.Services = append(doc.Services, model.Service{Key: s.Key, DisplayName: s.DisplayName, ServiceTypes: s.ServiceTypes, Traits: s.Traits})
	}
	prevEvidence := map[string]model.Evidence{}
	if prev != nil {
		for _, ps := range prev.Services {
			prevEvidence[ps.Key] = ps.Evidence
			if defined[ps.Key] {
				continue
			}
			// Dropped from the definition: kept until its removed items are purged.
			removedAt := ps.RemovedAt
			if removedAt == "" {
				removedAt = today
			}
			index[ps.Key] = len(doc.Services)
			doc.Services = append(doc.Services, model.Service{Key: ps.Key, DisplayName: ps.DisplayName,
				ServiceTypes: ps.ServiceTypes, Traits: ps.Traits, RemovedAt: removedAt})
		}
	}
	for i := range doc.Services {
		svc := &doc.Services[i]
		svc.Evidence = reconcileCurated(prevEvidence[svc.Key], curatedBy[svc.Key], today)
	}

	feedIDs := map[string]bool{}
	for _, ref := range def.Feeds {
		id := ref.ID()
		feedIDs[id] = true
		var prevStatus model.CollectorStatus
		if prev != nil {
			prevStatus = prev.Collection.Collectors[id]
		}
		prevItems := feedItems(prev, func(collector string) bool { return collector == id })
		outcome, ran := outcomes[id]
		if !ran {
			outcome = FeedOutcome{Err: errors.New("collector did not run")}
		}
		if outcome.Err == nil {
			doc.Collection.Collectors[id] = applyFeed(&doc, index, defined, ref, outcome.Result, prevItems, today, now)
		} else {
			doc.Collection.Collectors[id] = carryForward(&doc, index, prevItems, prevStatus, outcome.Err, now)
		}
	}
	// Feeds dropped from the definition: their ranges are removed now.
	for _, p := range feedItems(prev, func(collector string) bool { return !feedIDs[collector] }) {
		retire(&p.item.Lifecycle, model.ActionDefinitionRemoved, today)
		appendRange(&doc, index, p)
	}

	purge(&doc, today)
	model.Normalize(&doc)
	hash, err := model.ContentHash(doc)
	if err != nil {
		return model.Document{}, err
	}
	doc.Collection.ContentHash = hash
	return doc, nil
}

// Restore undoes wrong removals: every range of one feed that grace expiry
// removed on or after removedSince (YYYY-MM-DD) goes back to active, with
// RestoredAt recording the correction. Definition removals are never
// restored, and neither is an instance whose range already came back as a new
// live instance. It returns how many ranges it restored.
func Restore(doc model.Document, collectorID, removedSince string, now time.Time) (model.Document, int, error) {
	st, ok := doc.Collection.Collectors[collectorID]
	if !ok {
		return doc, 0, fmt.Errorf("%s has no collector %q", doc.Slug, collectorID)
	}
	if _, err := time.Parse(model.DateLayout, removedSince); err != nil {
		return doc, 0, fmt.Errorf("removed-since %q is not a YYYY-MM-DD date", removedSince)
	}
	cp, err := clone(doc)
	if err != nil {
		return doc, 0, err
	}
	today := now.UTC().Format(model.DateLayout)
	n := 0
	for i := range cp.Services {
		rs := cp.Services[i].Evidence.IPRanges
		live := map[string]bool{}
		for _, r := range rs {
			if r.Collector == collectorID && r.Status != model.StatusRemoved {
				live[r.CIDR] = true
			}
		}
		for j := range rs {
			r := &rs[j]
			if r.Collector != collectorID || r.Status != model.StatusRemoved || r.RemovalAction != model.ActionGraceExpired ||
				r.RemovedAt < removedSince || live[r.CIDR] {
				continue
			}
			r.Status, r.RemovedAt, r.RemovalAction, r.MissingSince, r.RestoredAt = model.StatusActive, "", "", "", today
			live[r.CIDR] = true
			n++
		}
	}
	if n == 0 {
		return doc, 0, fmt.Errorf("%s %s has no grace-expired removals on or after %s; nothing to restore", doc.Slug, collectorID, removedSince)
	}
	st.Items = countLive(&cp, collectorID)
	cp.Collection.Collectors[collectorID] = st
	model.Normalize(&cp)
	hash, err := model.ContentHash(cp)
	if err != nil {
		return doc, 0, err
	}
	cp.Collection.ContentHash = hash
	return cp, n, nil
}

func applyFeed(doc *model.Document, index map[string]int, defined map[string]bool, ref definitions.FeedRef,
	res feeds.Result, prevItems []svcRange, today string, now time.Time) model.CollectorStatus {
	id := ref.ID()
	source, confidence := model.SourceOfficialFeed, officialConfidence
	if bgpCollectors[ref.Collector] {
		source, confidence = model.SourceBGP, bgpConfidence
	}

	// Only a tag that resolves to one of this provider's services claims a
	// prefix away from the umbrella tag. Ignored ("") and unmapped tags do not,
	// or the prefix would vanish from the provider entirely.
	specific := map[netip.Prefix]bool{}
	for _, r := range res.Ranges {
		if ref.IsGeneric(r.Tag) {
			continue
		}
		if svcKey, ok := ref.ResolveTag(r.Tag); ok && svcKey != "" && defined[svcKey] {
			specific[r.Prefix] = true
		}
	}

	// This run's observations, one per (service, CIDR) within this feed.
	unmapped := map[string]bool{}
	observed := map[string]svcRange{}
	for _, r := range res.Ranges {
		if ref.IsGeneric(r.Tag) && specific[r.Prefix] {
			continue
		}
		svcKey, ok := ref.ResolveTag(r.Tag)
		if !ok || (svcKey != "" && !defined[svcKey]) {
			unmapped[r.Tag] = true
			continue
		}
		if svcKey == "" {
			continue
		}
		item := model.IPRange{CIDR: r.Prefix.String(), Region: r.Region, FeedTag: r.Tag, Confidence: confidence,
			Provenance: model.Provenance{Source: source, Collector: id, SourceURL: res.SourceURL, SourceVersion: res.SourceVersion}}
		k := svcKey + "|" + item.CIDR
		if cur, dup := observed[k]; dup && !better(item, cur.item) {
			continue
		}
		observed[k] = svcRange{svc: svcKey, item: item}
	}

	live := map[string]svcRange{}
	var liveKeys []string
	for _, p := range prevItems {
		if p.item.Status == model.StatusRemoved {
			appendRange(doc, index, p)
			continue
		}
		k := p.svc + "|" + p.item.CIDR
		if _, dup := live[k]; !dup {
			liveKeys = append(liveKeys, k)
		}
		live[k] = p
	}

	var churn model.Churn
	for _, k := range sortedMapKeys(observed) {
		o := observed[k]
		if p, ok := live[k]; ok {
			o.item.Lifecycle = p.item.Lifecycle
			if p.item.Status == model.StatusMissing {
				churn.Reappeared++
			}
			delete(live, k)
		} else {
			o.item.Lifecycle = model.Lifecycle{FirstSeen: today}
			churn.Added++
		}
		o.item.Status, o.item.LastSeen, o.item.MissingSince = model.StatusActive, today, ""
		appendRange(doc, index, o)
	}

	var missing []svcRange
	sort.Strings(liveKeys)
	for _, k := range liveKeys {
		p, ok := live[k]
		if !ok {
			continue
		}
		if !defined[p.svc] {
			retire(&p.item.Lifecycle, model.ActionDefinitionRemoved, today)
			churn.Removed++
			appendRange(doc, index, p)
			continue
		}
		if p.item.Status == model.StatusActive {
			p.item.Status, p.item.MissingSince = model.StatusMissing, today
			churn.Missing++
		}
		missing = append(missing, p)
	}
	for _, p := range missing {
		if daysBetween(p.item.MissingSince, today) >= ref.Grace() {
			retire(&p.item.Lifecycle, model.ActionGraceExpired, today)
			churn.Removed++
		}
		appendRange(doc, index, p)
	}

	success := now
	return model.CollectorStatus{
		Status: "ok", SourceURL: res.SourceURL, SourceVersion: res.SourceVersion,
		Items: countLive(doc, id), FetchedAt: now, LastSuccessAt: &success, SkippedLines: res.Skipped,
		UnmappedTags: sortedKeys(unmapped), Churn: churn,
	}
}

// carryForward keeps a failed feed's ranges exactly as they were: nothing can
// be observed missing, and nothing expires, while the feed is down.
func carryForward(doc *model.Document, index map[string]int, prevItems []svcRange, prevStatus model.CollectorStatus, cause error, now time.Time) model.CollectorStatus {
	items := 0
	for _, p := range prevItems {
		appendRange(doc, index, p)
		if p.item.Status != model.StatusRemoved {
			items++
		}
	}
	status := model.CollectorStatus{Status: "failed", FetchedAt: now, Error: cause.Error()}
	if prevStatus.LastSuccessAt == nil {
		return status
	}
	status.Status = "stale"
	status.SourceURL = prevStatus.SourceURL
	status.SourceVersion = prevStatus.SourceVersion
	status.Items = items
	status.LastSuccessAt = prevStatus.LastSuccessAt
	return status
}

func curatedProvenance(url string) model.Provenance {
	return model.Provenance{Source: model.SourceCurated, SourceURL: url}
}

func curatedEvidence(s definitions.ServiceDef) model.Evidence {
	var e model.Evidence
	for _, r := range s.IPRanges {
		e.IPRanges = append(e.IPRanges, model.IPRange{CIDR: r.CIDR, Region: r.Region,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, a := range s.ASNs {
		e.ASNs = append(e.ASNs, model.ASN{ASN: a.ASN,
			Confidence: definitions.ConfidenceOr(a.Confidence, curatedConfidence), Note: a.Note, Provenance: curatedProvenance(a.SourceURL)})
	}
	for _, r := range s.DNSRules {
		e.DNSRules = append(e.DNSRules, model.DNSRule{RecordType: r.RecordType, MatchField: r.MatchField,
			MatcherType: r.MatcherType, Pattern: r.Pattern, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, r := range s.HTTPRules {
		e.HTTPRules = append(e.HTTPRules, model.HTTPRule{HTTPPart: r.HTTPPart, HeaderName: r.HeaderName,
			MatcherType: r.MatcherType, Pattern: r.Pattern, PathScope: r.PathScope, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, r := range s.PTRRules {
		e.PTRRules = append(e.PTRRules, model.PTRRule{MatcherType: r.MatcherType, Pattern: r.Pattern,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, c := range s.CertificateIdentities {
		e.CertificateIdentities = append(e.CertificateIdentities, model.CertificateIdentity{IdentityType: c.IdentityType,
			IdentityValue: c.IdentityValue, Confidence: definitions.ConfidenceOr(c.Confidence, curatedConfidence),
			Note: c.Note, Provenance: curatedProvenance(c.SourceURL)})
	}
	return e
}

// reconcileCurated carries the lifecycle of curated items that are still
// defined, starts new ones, and removes the ones the definition dropped. Feed
// ranges are handled per feed, not here.
func reconcileCurated(prev, cur model.Evidence, today string) model.Evidence {
	return model.Evidence{
		IPRanges: reconcile(prev.IPRanges, cur.IPRanges, model.IPRange.Key,
			func(x *model.IPRange) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		ASNs: reconcile(prev.ASNs, cur.ASNs, model.ASN.Key,
			func(x *model.ASN) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		DNSRules: reconcile(prev.DNSRules, cur.DNSRules, model.DNSRule.Key,
			func(x *model.DNSRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		HTTPRules: reconcile(prev.HTTPRules, cur.HTTPRules, model.HTTPRule.Key,
			func(x *model.HTTPRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		PTRRules: reconcile(prev.PTRRules, cur.PTRRules, model.PTRRule.Key,
			func(x *model.PTRRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		CertificateIdentities: reconcile(prev.CertificateIdentities, cur.CertificateIdentities, model.CertificateIdentity.Key,
			func(x *model.CertificateIdentity) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
	}
}

func reconcile[T any](prev, cur []T, key func(T) string, parts func(*T) (*model.Lifecycle, *model.Provenance), today string) []T {
	out := make([]T, 0, len(cur))
	live := map[string]T{}
	var liveKeys []string
	for _, p := range prev {
		l, pv := parts(&p)
		if pv.Source != model.SourceCurated {
			continue
		}
		if l.Status == model.StatusRemoved {
			out = append(out, p)
			continue
		}
		k := key(p)
		if _, dup := live[k]; !dup {
			liveKeys = append(liveKeys, k)
		}
		live[k] = p
	}
	for _, c := range cur {
		k := key(c)
		l, _ := parts(&c)
		if p, ok := live[k]; ok {
			pl, _ := parts(&p)
			*l = *pl
			delete(live, k)
		} else {
			*l = model.Lifecycle{FirstSeen: today}
		}
		l.Status, l.LastSeen, l.MissingSince = model.StatusActive, today, ""
		out = append(out, c)
	}
	for _, k := range liveKeys {
		p, ok := live[k]
		if !ok {
			continue
		}
		l, _ := parts(&p)
		retire(l, model.ActionDefinitionRemoved, today)
		out = append(out, p)
	}
	return out
}

func retire(l *model.Lifecycle, action, today string) {
	if l.Status == model.StatusRemoved {
		return
	}
	l.Status, l.RemovedAt, l.RemovalAction = model.StatusRemoved, today, action
}

// feedItems returns the previous document's feed ranges whose collector
// matches.
func feedItems(prev *model.Document, match func(collector string) bool) []svcRange {
	if prev == nil {
		return nil
	}
	var out []svcRange
	for _, s := range prev.Services {
		for _, r := range s.Evidence.IPRanges {
			if r.Source != model.SourceCurated && r.Collector != "" && match(r.Collector) {
				out = append(out, svcRange{svc: s.Key, item: r})
			}
		}
	}
	return out
}

func appendRange(doc *model.Document, index map[string]int, p svcRange) {
	i, ok := index[p.svc]
	if !ok {
		index[p.svc] = len(doc.Services)
		doc.Services = append(doc.Services, model.Service{Key: p.svc, RemovedAt: p.item.RemovedAt})
		i = index[p.svc]
	}
	doc.Services[i].Evidence.IPRanges = append(doc.Services[i].Evidence.IPRanges, p.item)
}

func countLive(doc *model.Document, collector string) int {
	n := 0
	for _, s := range doc.Services {
		for _, r := range s.Evidence.IPRanges {
			if r.Collector == collector && r.Status != model.StatusRemoved {
				n++
			}
		}
	}
	return n
}

// better prefers a range with a region, then a stable tag/region order.
func better(a, b model.IPRange) bool {
	if (a.Region == "") != (b.Region == "") {
		return a.Region != ""
	}
	if a.FeedTag != b.FeedTag {
		return a.FeedTag < b.FeedTag
	}
	return a.Region < b.Region
}

// purge drops removed items older than the retention period, and services
// dropped from the definition once they hold no items.
func purge(doc *model.Document, today string) {
	out := doc.Services[:0:0]
	for _, s := range doc.Services {
		e := &s.Evidence
		e.IPRanges = keepFresh(e.IPRanges, func(x *model.IPRange) *model.Lifecycle { return &x.Lifecycle }, today, func(x model.IPRange) {
			if st, ok := doc.Collection.Collectors[x.Collector]; ok {
				st.Churn.Purged++
				doc.Collection.Collectors[x.Collector] = st
			}
		})
		e.ASNs = keepFresh(e.ASNs, func(x *model.ASN) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.DNSRules = keepFresh(e.DNSRules, func(x *model.DNSRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.HTTPRules = keepFresh(e.HTTPRules, func(x *model.HTTPRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.PTRRules = keepFresh(e.PTRRules, func(x *model.PTRRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.CertificateIdentities = keepFresh(e.CertificateIdentities, func(x *model.CertificateIdentity) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		if s.RemovedAt != "" && len(e.IPRanges)+len(e.ASNs)+len(e.DNSRules)+len(e.HTTPRules)+len(e.PTRRules)+len(e.CertificateIdentities) == 0 {
			continue
		}
		out = append(out, s)
	}
	doc.Services = out
}

func keepFresh[T any](items []T, life func(*T) *model.Lifecycle, today string, onPurge func(T)) []T {
	out := items[:0:0]
	for _, it := range items {
		l := life(&it)
		if l.Status == model.StatusRemoved && daysBetween(l.RemovedAt, today) > RemovedRetentionDays {
			if onPurge != nil {
				onPurge(it)
			}
			continue
		}
		out = append(out, it)
	}
	return out
}

// upgrade gives items published before lifecycle tracking an active lifecycle
// starting on the day of that run. It works on a copy.
func upgrade(prev *model.Document) *model.Document {
	if prev == nil {
		return nil
	}
	cp, err := clone(*prev)
	if err != nil {
		return prev
	}
	since := cp.Collection.CollectedAt.UTC().Format(model.DateLayout)
	fix := func(l *model.Lifecycle) {
		if l.Status == "" {
			*l = model.Lifecycle{Status: model.StatusActive, FirstSeen: since, LastSeen: since}
		}
	}
	for i := range cp.Services {
		e := &cp.Services[i].Evidence
		for j := range e.IPRanges {
			fix(&e.IPRanges[j].Lifecycle)
		}
		for j := range e.ASNs {
			fix(&e.ASNs[j].Lifecycle)
		}
		for j := range e.DNSRules {
			fix(&e.DNSRules[j].Lifecycle)
		}
		for j := range e.HTTPRules {
			fix(&e.HTTPRules[j].Lifecycle)
		}
		for j := range e.PTRRules {
			fix(&e.PTRRules[j].Lifecycle)
		}
		for j := range e.CertificateIdentities {
			fix(&e.CertificateIdentities[j].Lifecycle)
		}
	}
	return &cp
}

func clone(doc model.Document) (model.Document, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return model.Document{}, err
	}
	var cp model.Document
	err = json.Unmarshal(raw, &cp)
	return cp, err
}

func daysBetween(from, to string) int {
	a, err1 := time.Parse(model.DateLayout, from)
	b, err2 := time.Parse(model.DateLayout, to)
	if err1 != nil || err2 != nil {
		return 0
	}
	return int(b.Sub(a).Hours() / 24)
}

func sortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedMapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/assemble/ -v 2>&1 | rg -- "^(---|ok|FAIL)" && go test ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS. Both the new lifecycle tests and every existing `assemble_test.go` test pass unchanged.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/assemble
git commit -m "feat(provider_recon): reconcile evidence lifecycles with grace period, retention and restore"
```

---

### Task 5: Lifecycle diffs and a richer manifest

**Files:**
- Replace: `internal/publish/diff.go` (full content below)
- Modify: `internal/publish/publish.go`
- Test: `internal/publish/lifecycle_test.go` (new). The existing `publish_test.go` must keep passing unchanged.

**Interfaces:**
- Consumes: the Task 1 lifecycle; `assemble` is not imported.
- Produces:
  - `KindDiff` gains `Missing`, `Reappeared`, `Restored`, `Purged`, `Updated` lists with their `*Count` fields; the existing `Added`/`Removed`/`AddedCount`/`RemovedCount`/`Truncated` are kept.
  - `type Scope struct{ Command string; Providers []string }`
  - `type FeedRun struct{ Slug, Collector, Status string; Items int; Churn model.Churn; UnmappedTags []string; Error string }`
  - `Manifest` gains `Scope Scope` and `Feeds []FeedRun`.
  - `func RunID(now time.Time, command string) string`
  - `HistoryKey(slug, runID string) string` (signature change)
  - `func PublishScoped(ctx, store, docs, now, scope Scope) (Manifest, error)`; `Publish` = `PublishScoped` with `Scope{Command: "collect"}`.

- [ ] **Step 1: Write the failing tests** — `internal/publish/lifecycle_test.go`

```go
package publish

import (
	"context"
	"strings"
	"testing"
	"time"

	"provider_recon/internal/model"
)

func lcDoc(t *testing.T, items ...model.IPRange) model.Document {
	t.Helper()
	d := model.Document{Version: model.ContractVersion, Slug: "aws", DisplayName: "AWS", Category: "cloud",
		Services: []model.Service{{Key: "aws.cloudfront", ServiceTypes: []string{"cdn"}, Evidence: model.Evidence{IPRanges: items}}},
		Collection: model.Collection{Collectors: map[string]model.CollectorStatus{
			"aws_ip_ranges": {Status: "ok", Items: len(items), Churn: model.Churn{Added: 1}},
		}}}
	h, err := model.ContentHash(d)
	if err != nil {
		t.Fatal(err)
	}
	d.Collection.ContentHash = h
	return d
}

func ip(cidr, status, first string) model.IPRange {
	return model.IPRange{CIDR: cidr, Confidence: 1,
		Provenance: model.Provenance{Source: model.SourceOfficialFeed, Collector: "aws_ip_ranges"},
		Lifecycle:  model.Lifecycle{Status: status, FirstSeen: first, LastSeen: first}}
}

func TestDiffListsLifecycleTransitions(t *testing.T) {
	old := lcDoc(t, ip("10.0.1.0/24", "active", "2026-09-01"), ip("10.0.2.0/24", "missing", "2026-09-01"),
		ip("10.0.3.0/24", "active", "2026-09-01"), ip("10.0.4.0/24", "removed", "2026-06-01"),
		ip("10.0.6.0/24", "removed", "2026-09-01"))
	gone := ip("10.0.3.0/24", "removed", "2026-09-01")
	gone.RemovalAction = model.ActionGraceExpired
	back := ip("10.0.6.0/24", "active", "2026-09-01")
	back.RestoredAt = "2026-09-27"
	cur := lcDoc(t, ip("10.0.1.0/24", "missing", "2026-09-01"), ip("10.0.2.0/24", "active", "2026-09-01"),
		gone, ip("10.0.5.0/24", "active", "2026-09-27"), back)
	d := Diff(&old, cur).Evidence["ip_ranges"]
	check := func(name string, got []string, want string) {
		t.Helper()
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want [%s]", name, got, want)
		}
	}
	check("missing", d.Missing, "aws.cloudfront 10.0.1.0/24")
	check("reappeared", d.Reappeared, "aws.cloudfront 10.0.2.0/24")
	check("removed", d.Removed, "aws.cloudfront 10.0.3.0/24 (grace_expired)")
	check("purged", d.Purged, "aws.cloudfront 10.0.4.0/24")
	check("added", d.Added, "aws.cloudfront 10.0.5.0/24")
	check("restored", d.Restored, "aws.cloudfront 10.0.6.0/24")
}

func TestDiffLastSeenAloneIsNotAnUpdate(t *testing.T) {
	a := ip("10.0.1.0/24", "active", "2026-09-01")
	b := a
	b.LastSeen = "2026-09-27"
	old, cur := lcDoc(t, a), lcDoc(t, b)
	if d, ok := Diff(&old, cur).Evidence["ip_ranges"]; ok {
		t.Fatalf("last_seen-only change reported: %+v", d)
	}
	c := a
	c.Region = "eu-north-1"
	if d := Diff(&old, lcDoc(t, c)).Evidence["ip_ranges"]; len(d.Updated) != 1 {
		t.Fatalf("region change not reported as updated: %+v", d)
	}
}

func TestPublishScopedManifestHasScopeAndFeeds(t *testing.T) {
	ctx := context.Background()
	store := FSStore{Root: t.TempDir()}
	now := time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)
	d := lcDoc(t, ip("10.0.1.0/24", "active", "2026-09-27"))
	d.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "stale", Items: 1, Error: "status 503"}
	m, err := PublishScoped(ctx, store, []model.Document{d}, now, Scope{Command: "collect", Providers: []string{"aws"}})
	if err != nil {
		t.Fatal(err)
	}
	if m.RunID != "20260927T060000Z-collect" || m.Scope.Command != "collect" || len(m.Scope.Providers) != 1 {
		t.Fatalf("manifest scope = %+v run=%s", m.Scope, m.RunID)
	}
	if len(m.Feeds) != 1 || m.Feeds[0].Status != "stale" || m.Feeds[0].Collector != "aws_ip_ranges" {
		t.Fatalf("feeds = %+v", m.Feeds)
	}
	if len(m.CollectorIssues) != 1 || m.CollectorIssues[0].Status != "stale" || m.CollectorIssues[0].Error != "status 503" {
		t.Fatalf("issues = %+v", m.CollectorIssues)
	}
	if _, ok, _ := store.Get(ctx, HistoryKey("aws", m.RunID)); !ok {
		t.Fatal("history not keyed by run id")
	}
	a, err := PublishScoped(ctx, store, []model.Document{d}, now, Scope{Command: "restore", Providers: []string{"aws"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.RunID == m.RunID || !strings.HasSuffix(a.RunID, "-restore") {
		t.Fatalf("restore run id %q collides with collect run %q", a.RunID, m.RunID)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/publish/`
Expected: FAIL (`undefined: PublishScoped`, `d.Missing undefined`, …).

- [ ] **Step 3: Replace** `internal/publish/diff.go`

```go
package publish

import (
	"encoding/json"
	"sort"

	"provider_recon/internal/model"
)

// maxListed caps the item lists in a change; counts stay exact.
const maxListed = 1000

// KindDiff is one evidence kind's lifecycle transitions in a run. Items are
// labelled "<service> <key>"; removals carry the action in parentheses;
// restored means restore undid a removal; purged means the item left the
// document (retention, or an alias/key edit).
type KindDiff struct {
	Added           []string `json:"added,omitempty"`
	Missing         []string `json:"missing,omitempty"`
	Reappeared      []string `json:"reappeared,omitempty"`
	Restored        []string `json:"restored,omitempty"`
	Removed         []string `json:"removed,omitempty"`
	Purged          []string `json:"purged,omitempty"`
	Updated         []string `json:"updated,omitempty"`
	AddedCount      int      `json:"added_count"`
	MissingCount    int      `json:"missing_count"`
	ReappearedCount int      `json:"reappeared_count"`
	RestoredCount   int      `json:"restored_count"`
	RemovedCount    int      `json:"removed_count"`
	PurgedCount     int      `json:"purged_count"`
	UpdatedCount    int      `json:"updated_count"`
	Truncated       bool     `json:"truncated,omitempty"`
}

func (d KindDiff) empty() bool {
	return d.AddedCount+d.MissingCount+d.ReappearedCount+d.RestoredCount+d.RemovedCount+d.PurgedCount+d.UpdatedCount == 0
}

// VersionChange is a feed version before and after the run.
type VersionChange struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// ProviderChange describes what changed for one provider in a run.
type ProviderChange struct {
	Slug         string                   `json:"slug"`
	Created      bool                     `json:"created,omitempty"`
	OldHash      string                   `json:"old_hash,omitempty"`
	NewHash      string                   `json:"new_hash"`
	Evidence     map[string]KindDiff      `json:"evidence,omitempty"`
	FeedVersions map[string]VersionChange `json:"feed_versions,omitempty"`
}

var kinds = []string{"aliases", "asns", "certificate_identities", "dns_rules", "http_rules", "ip_ranges", "provider_keys", "ptr_rules", "services"}

// entry is one item instance; identity = label + first_seen, so a removed
// instance and its re-added successor are distinct.
type entry struct {
	label, status, action, canon string
}

type itemSet map[string]entry

// Diff compares cur with the previous document (nil for a new provider). A
// new provider reports counts only.
func Diff(old *model.Document, cur model.Document) ProviderChange {
	ch := ProviderChange{Slug: cur.Slug, NewHash: cur.Collection.ContentHash, Created: old == nil,
		Evidence: map[string]KindDiff{}, FeedVersions: map[string]VersionChange{}}
	if old != nil {
		ch.OldHash = old.Collection.ContentHash
	}
	before, after := itemSets(old), itemSets(&cur)
	for _, kind := range kinds {
		if d := diffKind(before[kind], after[kind], !ch.Created); !d.empty() {
			ch.Evidence[kind] = d
		}
	}
	for id, st := range cur.Collection.Collectors {
		prev := ""
		if old != nil {
			prev = old.Collection.Collectors[id].SourceVersion
		}
		if prev != st.SourceVersion {
			ch.FeedVersions[id] = VersionChange{Old: prev, New: st.SourceVersion}
		}
	}
	return ch
}

func diffKind(before, after itemSet, list bool) KindDiff {
	var added, missing, reappeared, restored, removed, purged, updated []string
	for id, a := range after {
		b, ok := before[id]
		switch {
		case !ok && a.status == model.StatusRemoved:
			removed = append(removed, a.label+" ("+a.action+")")
		case !ok:
			added = append(added, a.label)
		case b.status != model.StatusMissing && a.status == model.StatusMissing:
			missing = append(missing, a.label)
		case b.status == model.StatusMissing && a.status == model.StatusActive:
			reappeared = append(reappeared, a.label)
		case b.status == model.StatusRemoved && a.status == model.StatusActive:
			restored = append(restored, a.label)
		case b.status != model.StatusRemoved && a.status == model.StatusRemoved:
			removed = append(removed, a.label+" ("+a.action+")")
		case b.canon != a.canon:
			updated = append(updated, a.label)
		}
	}
	for id, b := range before {
		if _, ok := after[id]; !ok {
			purged = append(purged, b.label)
		}
	}
	d := KindDiff{AddedCount: len(added), MissingCount: len(missing), ReappearedCount: len(reappeared),
		RestoredCount: len(restored), RemovedCount: len(removed), PurgedCount: len(purged), UpdatedCount: len(updated)}
	if !list {
		return d
	}
	var t [7]bool
	d.Added, t[0] = capList(added)
	d.Missing, t[1] = capList(missing)
	d.Reappeared, t[2] = capList(reappeared)
	d.Restored, t[3] = capList(restored)
	d.Removed, t[4] = capList(removed)
	d.Purged, t[5] = capList(purged)
	d.Updated, t[6] = capList(updated)
	d.Truncated = t[0] || t[1] || t[2] || t[3] || t[4] || t[5] || t[6]
	return d
}

func capList(xs []string) ([]string, bool) {
	sort.Strings(xs)
	if len(xs) > maxListed {
		return xs[:maxListed], true
	}
	return xs, false
}

func itemSets(doc *model.Document) map[string]itemSet {
	sets := map[string]itemSet{}
	for _, k := range kinds {
		sets[k] = itemSet{}
	}
	if doc == nil {
		return sets
	}
	for _, a := range doc.Aliases {
		sets["aliases"][a] = entry{label: a, status: model.StatusActive, canon: a}
	}
	for _, k := range doc.ProviderKeys {
		sets["provider_keys"][k] = entry{label: k, status: model.StatusActive, canon: k}
	}
	for _, s := range doc.Services {
		status := model.StatusActive
		if s.RemovedAt != "" {
			status = model.StatusRemoved
		}
		sets["services"][s.Key] = entry{label: s.Key, status: status, action: model.ActionDefinitionRemoved, canon: s.Key}
		e := s.Evidence
		addItems(sets["ip_ranges"], s.Key, e.IPRanges, model.IPRange.Key,
			func(x *model.IPRange) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["asns"], s.Key, e.ASNs, model.ASN.Key,
			func(x *model.ASN) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["dns_rules"], s.Key, e.DNSRules, model.DNSRule.Key,
			func(x *model.DNSRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["http_rules"], s.Key, e.HTTPRules, model.HTTPRule.Key,
			func(x *model.HTTPRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["ptr_rules"], s.Key, e.PTRRules, model.PTRRule.Key,
			func(x *model.PTRRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["certificate_identities"], s.Key, e.CertificateIdentities, model.CertificateIdentity.Key,
			func(x *model.CertificateIdentity) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
	}
	return sets
}

// addItems records each instance. canon is the item with the fields that move
// without the evidence changing blanked (last_seen, source version, a feed
// item's source URL), so "updated" means a real field change.
func addItems[T any](set itemSet, svc string, items []T, key func(T) string, parts func(*T) (*model.Lifecycle, *model.Provenance)) {
	for _, it := range items {
		l, p := parts(&it)
		label := svc + " " + key(it)
		e := entry{label: label, status: l.Status, action: l.RemovalAction}
		l.LastSeen = ""
		p.SourceVersion = ""
		if p.Source != model.SourceCurated {
			p.SourceURL = ""
		}
		canon, _ := json.Marshal(it)
		e.canon = string(canon)
		set[label+" @"+l.FirstSeen] = e
	}
}
```

- [ ] **Step 4: Modify** `internal/publish/publish.go`

Replace the `HistoryKey` function with:
```go
// RunID names a run: its time plus the command, so a restore right after a
// collect never overwrites that run's objects.
func RunID(now time.Time, command string) string {
	return now.UTC().Format(RunIDLayout) + "-" + command
}

// HistoryKey is the copy written when a provider's content hash changes.
func HistoryKey(slug, runID string) string {
	return "providers/" + slug + "/history/" + runID + ".json"
}
```

Add after `CollectorIssue`:
```go
// Scope says what produced a run.
type Scope struct {
	Command   string   `json:"command"`             // collect | restore
	Providers []string `json:"providers,omitempty"` // empty: every definition
}

// FeedRun is one feed's outcome for one provider in a run; churn is what the
// hold thresholds get calibrated from.
type FeedRun struct {
	Slug         string      `json:"slug"`
	Collector    string      `json:"collector"`
	Status       string      `json:"status"`
	Items        int         `json:"items"`
	Churn        model.Churn `json:"churn"`
	UnmappedTags []string    `json:"unmapped_tags,omitempty"`
	Error        string      `json:"error,omitempty"`
}
```

In `Manifest`, add after `RunID`:
```go
	Scope           Scope            `json:"scope"`
```
and after `CollectorIssues`:
```go
	Feeds           []FeedRun        `json:"feeds"`
```

Replace the start of the `Publish` function (its doc comment and signature through the `sorted` line) with:
```go
// Publish is PublishScoped for a full collect run.
func Publish(ctx context.Context, store Store, docs []model.Document, now time.Time) (Manifest, error) {
	return PublishScoped(ctx, store, docs, now, Scope{Command: "collect"})
}

// PublishScoped runs in three phases so an interrupted run is always
// re-detected: (1) history objects for every changed provider, (2) the run's
// change manifest, (3) every latest.json. latest.json is what the next run
// compares against, so until phase 3 completes every change of this run is
// reported again next time rather than lost from the manifest stream.
func PublishScoped(ctx context.Context, store Store, docs []model.Document, now time.Time, scope Scope) (Manifest, error) {
	m := Manifest{RunID: RunID(now, scope.Command), Scope: scope, PublishedAt: now.UTC(),
		Changed: []ProviderChange{}, Unchanged: []string{}, CollectorIssues: []CollectorIssue{}, Feeds: []FeedRun{}}
	sorted := append([]model.Document(nil), docs...)
```

In the loop body:
- Change `HistoryKey(doc.Slug, now)` to `HistoryKey(doc.Slug, m.RunID)`.
- Replace the per-collector loop body with:

```go
		for _, id := range ids {
			st := doc.Collection.Collectors[id]
			m.Feeds = append(m.Feeds, FeedRun{Slug: doc.Slug, Collector: id, Status: st.Status, Items: st.Items,
				Churn: st.Churn, UnmappedTags: st.UnmappedTags, Error: st.Error})
			if st.Status != "ok" {
				m.CollectorIssues = append(m.CollectorIssues, CollectorIssue{Slug: doc.Slug, Collector: id, Status: st.Status, Error: st.Error})
			}
		}
```


- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/publish/ -v 2>&1 | rg -- "^(---|ok|FAIL)" && go test ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS. The new tests pass, and the existing `publish_test.go` passes unchanged (it uses `Publish`, `AddedCount`, `Added` and file counts, all kept). If `cmd/provider-recon` fails to compile, it's because `HistoryKey`'s signature changed; nothing in `main.go` calls it, so any such error is a real finding.

- [ ] **Step 6: Commit**

```bash
git add services/provider_recon/internal/publish
git commit -m "feat(provider_recon): lifecycle transitions, run scope and per-feed churn in the change manifest"
```

---

### Task 6: `restore` command and scoped collect

**Files:**
- Modify: `cmd/provider-recon/main.go`
- Modify test: `cmd/provider-recon/main_test.go`. Add a v6 prefix to the AWS body in `TestCollectTwiceIsIdempotent`, because the Task 2 shape check needs it. Add the new tests below.

**Interfaces:**
- Consumes: `assemble.Build`, `assemble.Restore` (Task 4); `publish.PublishScoped`, `publish.Scope` (Task 5).
- Produces:
  - `provider-recon restore -provider <slug> -collector <id> -removed-since <YYYY-MM-DD> [-out dir]`. Exit 0 on success, 1 on error, 64 on usage.
  - `collect` passes `Scope{Command:"collect", Providers:[slug]}` when `-provider` is given.

- [ ] **Step 1: Write the failing tests**

In `TestCollectTwiceIsIdempotent`, change the served body to:
```go
	withAWSServer(t, `{"syncToken":"1","prefixes":[{"ip_prefix":"52.84.0.0/15","region":"GLOBAL","service":"CLOUDFRONT"}],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`)
```

Append to `main_test.go`:
```go
func awsBody(v4 int) string {
	var b strings.Builder
	b.WriteString(`{"syncToken":"1","prefixes":[`)
	for i := 0; i < v4; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"ip_prefix":"10.0.%d.0/24","region":"GLOBAL","service":"CLOUDFRONT"}`, i)
	}
	b.WriteString(`],"ipv6_prefixes":[{"ipv6_prefix":"2600:9000::/28","region":"GLOBAL","service":"CLOUDFRONT"}]}`)
	return b.String()
}

// seedRemoval publishes, into out, an aws document whose range 10.0.9.0/24
// was removed by grace expiry two days ago, using the real Build and Publish.
func seedRemoval(t *testing.T, out string) string {
	t.Helper()
	def := definitions.Definition{Slug: "aws", DisplayName: "Amazon Web Services", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"}}},
		Feeds:    []definitions.FeedRef{{Collector: "aws_ip_ranges", TagMap: map[string]string{"CLOUDFRONT": "aws.cloudfront"}}}}
	outcome := func(n int) map[string]assemble.FeedOutcome {
		var rs []feeds.Range
		for i := 0; i < n; i++ {
			rs = append(rs, feeds.Range{Prefix: netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", i)), Tag: "CLOUDFRONT"})
		}
		rs = append(rs, feeds.Range{Prefix: netip.MustParsePrefix("2600:9000::/28"), Tag: "CLOUDFRONT"})
		return map[string]assemble.FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{Ranges: rs}}}
	}
	ctx := context.Background()
	store := publish.FSStore{Root: out}
	start := time.Now().UTC().AddDate(0, 0, -10)
	var prev *model.Document
	for _, step := range []struct{ day, ranges int }{{0, 10}, {1, 9}, {8, 9}} {
		at := start.AddDate(0, 0, step.day)
		doc, err := assemble.Build(def, outcome(step.ranges), prev, at)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := publish.Publish(ctx, store, []model.Document{doc}, at); err != nil {
			t.Fatal(err)
		}
		prev = &doc
	}
	return start.AddDate(0, 0, 8).Format(model.DateLayout)
}

func TestRestoreThenCollectContinuesTheTimeline(t *testing.T) {
	withAWSServer(t, awsBody(10))
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	out := t.TempDir()
	removedOn := seedRemoval(t, out)

	var stdout, stderr bytes.Buffer
	args := []string{"restore", "-provider", "aws", "-collector", "aws_ip_ranges", "-removed-since", removedOn, "-out", out}
	if code := run(context.Background(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("restore exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "restored 1 ranges for aws aws_ip_ranges") {
		t.Fatalf("restore stdout = %s", stdout.String())
	}

	stdout.Reset()
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("collect after restore exit %d: %s", code, stderr.String())
	}
	doc, err := publish.LoadLatest(context.Background(), publish.FSStore{Root: out}, "aws")
	if err != nil || doc == nil {
		t.Fatalf("latest: %v %v", doc, err)
	}
	var found []model.IPRange
	for _, s := range doc.Services {
		for _, r := range s.Evidence.IPRanges {
			if r.CIDR == "10.0.9.0/24" {
				found = append(found, r)
			}
		}
	}
	if len(found) != 1 || found[0].Status != model.StatusActive || found[0].RestoredAt == "" ||
		found[0].FirstSeen != time.Now().UTC().AddDate(0, 0, -10).Format(model.DateLayout) {
		t.Fatalf("10.0.9.0/24 after restore + collect = %+v", found)
	}
}

func TestRestoreUsageAndErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"restore", "-provider", "aws", "-collector", "x"}, &stdout, &stderr); code != 64 {
		t.Fatalf("missing -removed-since: exit %d", code)
	}
	args := []string{"restore", "-provider", "aws", "-collector", "x", "-removed-since", "2026-09-27", "-out", t.TempDir()}
	if code := run(context.Background(), args, &stdout, &stderr); code != 1 {
		t.Fatalf("no published document: exit %d", code)
	}
}
```
Add to the test imports: `"fmt"`, `"net/netip"`, `"time"`, `"provider_recon/internal/assemble"`, `"provider_recon/internal/definitions"`, `"provider_recon/internal/model"`, `"provider_recon/internal/publish"`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/provider-recon/`
Expected: FAIL. `restore` is an unknown command (exit 64), so both new tests fail.

- [ ] **Step 3: Implement** in `cmd/provider-recon/main.go`

In `run`, add the case:
```go
	case "restore":
		return cmdRestore(ctx, args[1:], stdout, stderr)
```
Change `usage` to print `usage: provider-recon validate|schema|collect|restore [flags]`.

In `cmdCollect`, replace `m, err := publish.Publish(ctx, store, docs, now)` with:
```go
	scope := publish.Scope{Command: "collect"}
	if *only != "" {
		scope.Providers = []string{*only}
	}
	m, err := publish.PublishScoped(ctx, store, docs, now, scope)
```

Add the command:
```go
// cmdRestore undoes wrong removals. Fix the collector first, restore, then
// collect: ranges the feed lists again continue their original timeline.
func cmdRestore(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	slug := fs.String("provider", "", "provider slug")
	collector := fs.String("collector", "", "collector id as listed in the manifest, e.g. aws_ip_ranges")
	since := fs.String("removed-since", "", "restore ranges removed on or after this day (YYYY-MM-DD)")
	outDir := fs.String("out", "", "use this local directory instead of S3")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	if *slug == "" || *collector == "" || *since == "" {
		fmt.Fprintln(stderr, "restore needs -provider, -collector and -removed-since")
		return 64
	}
	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	prev, err := publish.LoadLatest(ctx, store, *slug)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if prev == nil {
		fmt.Fprintf(stderr, "no published document for %q\n", *slug)
		return 1
	}
	now := time.Now().UTC().Truncate(time.Second)
	doc, n, err := assemble.Restore(*prev, *collector, *since, now)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	m, err := publish.PublishScoped(ctx, store, []model.Document{doc}, now, publish.Scope{Command: "restore", Providers: []string{*slug}})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "restored %d ranges for %s %s (run %s)\n", n, *slug, *collector, m.RunID)
	return 0
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./... && go vet ./... && test -z "$(gofmt -l .)" && make build`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/cmd
git commit -m "feat(provider_recon): restore command and scoped collect runs"
```

---

### Task 7: Azure grace period, README, real runs

**Files:**
- Modify: `definitions/microsoft.yaml`, `README.md`

- [ ] **Step 1: Azure grace period**

In `definitions/microsoft.yaml`, under the `azure_service_tags` feed, after `generic_tags`, add:
```yaml
    # Azure republishes weekly: two publications before a range is removed.
    removal_grace_days: 14
```
Run: `go run ./cmd/provider-recon validate`. Expected: `37 definitions valid`.

- [ ] **Step 2: README section**

Add before `## Environment` in `README.md`:
````markdown
## Removal lifecycle

Nothing disappears the moment a feed stops listing it. Every item has a
`status`:
- `active`
- `missing`: absent from a successful fetch since `missing_since`
- `removed`: with `removed_at` and a `removal_action` of `grace_expired` or
  `definition_removed`

Items also carry `first_seen` and `last_seen`.

- A missing range is removed after the feed's `removal_grace_days`
  (default 7; Azure 14). While a feed fails, nothing moves.
- No threshold blocks a large drop. Every run's per-feed churn is in the
  change manifest, and the backoffice highlights unusual updates.
- A wrong removal is undone with:

  ```bash
  bin/provider-recon restore -provider <slug> -collector <id> -removed-since <YYYY-MM-DD>
  ```

  It sets `restored_at`, and restores only grace-expired removals. Fix the
  collector first, then restore, then collect.
- Removed items stay in `latest.json` for 90 days. History objects keep them
  forever.
````
Add to the `## Commands` block:
```bash
bin/provider-recon restore -provider aws -collector aws_ip_ranges -removed-since 2026-10-03   # undo wrong removals
```

- [ ] **Step 3: Real runs, locally and on S3**

```bash
OUT=$(mktemp -d)
go run ./cmd/provider-recon collect -out "$OUT"; echo "exit=$?"
go run ./cmd/provider-recon collect -out "$OUT"; echo "exit=$?"
jq -c '.feeds[] | select(.slug=="aws" or .slug=="microsoft") | {slug, collector, status, items, churn}' "$OUT"/changes/*-collect.json | tail -4
jq -c '[.services[].evidence.ip_ranges[0] | select(.) | {status, first_seen, last_seen}][0]' "$OUT"/providers/aws/latest.json
set -a; source ../backoffice/.env; set +a
go run ./cmd/provider-recon collect; echo "exit=$?"
go run ./cmd/provider-recon collect; echo "exit=$?"
```
Expected:
- Local: first run 37 changed, second run 0 changed; both exit 0; feeds `ok`. The second run's churn is all zero. Ranges carry `status: active` with `first_seen` = today.
- S3, first run: 37 changed. This is a one-time upgrade: the slice-1 documents gain lifecycle fields, and the items are upgraded with `first_seen` = the slice-1 publish day (2026-09-27). Exit 0.
- S3, second run: 0 changed, exit 0.

- [ ] **Step 4: Commit**

```bash
git add services/provider_recon/definitions/microsoft.yaml services/provider_recon/README.md
git commit -m "feat(provider_recon): Azure 14-day removal grace and lifecycle docs"
```
