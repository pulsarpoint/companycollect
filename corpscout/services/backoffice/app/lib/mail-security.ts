// Mail-security scoring computed from crawled DNS records.
//
// This is a TypeScript port of runner3's mail function
// (pulsarprotectrunner/internal/functions/mail_function/function.go,
// `deriveOutput` + `buildControl`) and the record-analysis detection rules of
// its per-control scanners (spf_scanner, dmarc_scanner, mx_scanner,
// mta_sts_scanner, bimi_scanner). Unlike runner3 there are
// NO live scan steps here: everything is derived from DNS records already
// observed by the crawl, so
//   - the Go `FailedSteps * 5` penalty and the "step did not complete" fail
//     branch are dropped (there are no steps), and
//   - a control whose input data cannot exist in the crawl (DKIM selectors,
//     TLSA names that were never queried) is `unknown` instead of `fail`.
//
// Pure functions only -- this module is client-safe and must not import from
// any `.server` module.

/** One deduplicated DNS record from the crawl. TXT values may carry
 * surrounding double quotes and long records may be split into multiple
 * quoted chunks -- `normalizeTxtValue` strips/joins them. */
export type MailDnsRecord = {
  name: string;
  record_type: string;
  value: string;
  first_seen?: string;
  last_seen?: string;
};

export type MailControlStatus = "pass" | "warn" | "fail" | "unknown";

export type MailSecurityControl = {
  key: string;
  label: string;
  status: MailControlStatus;
  present: boolean;
  /** Human-readable judgments; the first entry mirrors the Go control
   * reason, further entries name the individual detections. */
  reasons: string[];
  /** The record values behind the judgment, one string per record. */
  evidence: string[];
};

export type MailSecuritySummary = {
  mx_found: boolean;
  mx_host_count: number;
  spf_found: boolean;
  dmarc_found: boolean;
  dkim_records_found: number;
  mta_sts_found: boolean;
  bimi_found: boolean;
  dnssec_available: boolean;
  total_detections: number;
};

export type MailSecurityReport = {
  domain: string;
  score: number;
  mail_ready: boolean;
  summary: MailSecuritySummary;
  controls: MailSecurityControl[];
  /** Max `last_seen` over the records that fed the judgment; "" if none. */
  last_seen: string;
  /** True when no DKIM selectors were observed in the crawl: the DKIM
   * control is shown but EXCLUDED from the score (owner 2026-08-29). */
  scored_without_dkim: boolean;
};

/** Strip surrounding quotes from a crawled TXT value and join multi-chunk
 * records (`"part one" "part two"` -> `part onepart two`, per RFC 1035
 * character-string concatenation). Unquoted values pass through. */
export function normalizeTxtValue(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed.startsWith('"')) return trimmed;
  const chunks = trimmed.match(/"((?:[^"\\]|\\.)*)"/g);
  if (!chunks) return trimmed;
  return chunks
    .map((chunk) => chunk.slice(1, -1).replace(/\\(.)/g, "$1"))
    .join("");
}

function canonicalName(name: string): string {
  return name.trim().toLowerCase().replace(/\.$/, "");
}

type ControlDraft = {
  key: string;
  label: string;
  present: boolean;
  detections: string[];
  reasons: string[];
  evidence: string[];
  /** Overrides the pass/warn/fail derivation entirely (unobserved DKIM). */
  forcedStatus?: MailControlStatus;
};

/** Port of Go `buildControl` minus the step-status branch: fail when absent
 * or when detections exist without presence; warn when present WITH
 * detections; pass when present clean. */
function finalizeControl(draft: ControlDraft): MailSecurityControl {
  let status: MailControlStatus;
  let reason: string;
  if (draft.forcedStatus !== undefined) {
    status = draft.forcedStatus;
    reason = draft.reasons[0] ?? "status forced";
    return {
      key: draft.key,
      label: draft.label,
      status,
      present: draft.present,
      reasons: draft.reasons.length > 0 ? draft.reasons : [reason],
      evidence: draft.evidence,
    };
  }
  if (draft.detections.length > 0 && draft.present) {
    status = "warn";
    reason = `${draft.detections.length} issue(s) detected`;
  } else if (draft.detections.length > 0) {
    status = "fail";
    reason = `${draft.detections.length} issue(s) detected`;
  } else if (draft.present) {
    status = "pass";
    reason = "capability present and evaluated";
  } else {
    status = "fail";
    reason = "capability not found";
  }
  return {
    key: draft.key,
    label: draft.label,
    status,
    present: draft.present,
    reasons: [reason, ...draft.detections, ...draft.reasons],
    evidence: draft.evidence,
  };
}

// ---------------------------------------------------------------------------
// SPF record analysis (port of spf_scanner parseRecord + buildIssues).
//
// Ported rules: spf_missing, spf_multiple_records, spf_syntax_error
// (lightweight term grammar + the supplemental duplicate-'all' /
// token-after-'all' checks), spf_ptr_mechanism, spf_allows_all (+all),
// spf_weak (~all), spf_lookups_high/excessive/critical. The Go scanner
// counts DNS lookups over TOP-LEVEL terms only (include/a/mx/ptr/exists/
// redirect, no recursion into includes), so the lookup tiers are a faithful
// record-text-only port -- nothing to skip.
//
// Deliberately skipped: the `MailSendingDomain == false` branch
// (nomail_spf_not_reject_all) -- whether a domain intends to send mail is an
// operator intent flag runner3 receives in its request; the crawl carries no
// such signal, so every domain is evaluated as a mail-sending domain.

type SpfAnalysis = {
  syntaxErrors: string[];
  ptrMechanisms: string[];
  allQualifier: string | null; // "+", "-", "~", "?" -- null when no `all`
  dnsLookups: number;
  valid: boolean;
};

const SPF_SIMPLE_MECHANISMS = new Set(["all", "a", "mx", "ptr"]);
const SPF_VALUE_MECHANISMS = new Set([
  "include",
  "a",
  "mx",
  "ptr",
  "ip4",
  "ip6",
  "exists",
]);
const SPF_LOOKUP_MECHANISMS = new Set(["include", "a", "mx", "ptr", "exists"]);

function analyzeSpfRecord(record: string): SpfAnalysis {
  const analysis: SpfAnalysis = {
    syntaxErrors: [],
    ptrMechanisms: [],
    allQualifier: null,
    dnsLookups: 0,
    valid: true,
  };
  const tokens = record.trim().split(/\s+/);
  // tokens[0] is v=spf1 (the caller selected the record by that prefix).
  let sawAll = false;
  for (const token of tokens.slice(1)) {
    if (token === "") continue;
    let qualifier = "+";
    let body = token;
    if ("+-~?".includes(token[0]!)) {
      qualifier = token[0]!;
      body = token.slice(1);
    }
    const lower = body.toLowerCase();
    const mechanismName = lower.split(/[:/]/, 1)[0] ?? "";

    if (sawAll && lower !== "all") {
      analysis.syntaxErrors.push(
        `token "${token}" appears after 'all' and will never be evaluated`,
      );
    }
    if (lower === "all") {
      if (sawAll) {
        analysis.syntaxErrors.push(
          "record contains more than one 'all' terminator",
        );
      } else {
        analysis.allQualifier = qualifier;
      }
      sawAll = true;
      continue;
    }

    // Modifiers: name=value where name has no ':' before the '='.
    const eq = body.indexOf("=");
    if (eq > 0 && !body.slice(0, eq).includes(":")) {
      const modifier = body.slice(0, eq).toLowerCase();
      if (modifier === "redirect") analysis.dnsLookups += 1;
      if (!/^[a-z][a-z0-9._-]*$/.test(modifier)) {
        analysis.syntaxErrors.push(`invalid modifier name in "${token}"`);
      }
      continue;
    }

    if (SPF_LOOKUP_MECHANISMS.has(mechanismName)) analysis.dnsLookups += 1;
    if (mechanismName === "ptr") analysis.ptrMechanisms.push(body);

    const hasValue = lower.includes(":");
    if (hasValue) {
      if (!SPF_VALUE_MECHANISMS.has(mechanismName)) {
        analysis.syntaxErrors.push(`unknown mechanism "${token}"`);
      } else if (lower.split(":", 2)[1] === "") {
        analysis.syntaxErrors.push(`mechanism "${token}" is missing a value`);
      }
    } else if (!SPF_SIMPLE_MECHANISMS.has(mechanismName)) {
      // Covers ip4/ip6/include/exists without a value too: those require
      // `:<value>` and are not valid as bare terms.
      analysis.syntaxErrors.push(`unknown term "${token}"`);
    }
  }
  if (analysis.dnsLookups > 10) analysis.valid = false;
  if (analysis.syntaxErrors.length > 0) analysis.valid = false;
  return analysis;
}

function buildSpfControl(
  spfTxtRecords: string[],
  legacySpfTypeCount: number,
): ControlDraft {
  const draft: ControlDraft = {
    key: "spf",
    label: "SPF",
    present: spfTxtRecords.length > 0,
    detections: [],
    reasons: [],
    evidence: [...spfTxtRecords],
  };

  if (spfTxtRecords.length === 0) {
    // Legacy type-SPF records are counted as presence only (RFC 7208
    // retired the RR type; receivers ignore it) -- no record analysis.
    if (legacySpfTypeCount > 0) {
      draft.present = true;
      draft.reasons.push(
        "record published only under the deprecated SPF record type",
      );
      return draft;
    }
    draft.detections.push("no SPF record found (spf_missing)");
    return draft;
  }

  if (spfTxtRecords.length > 1) {
    draft.detections.push(
      `${spfTxtRecords.length} SPF records published; RFC 7208 permits exactly one (spf_multiple_records)`,
    );
  }

  const record = spfTxtRecords[0]!;
  const analysis = analyzeSpfRecord(record);

  if (analysis.syntaxErrors.length > 0) {
    draft.detections.push(
      `SPF syntax error: ${analysis.syntaxErrors[0]} (spf_syntax_error)`,
    );
  }
  if (analysis.ptrMechanisms.length > 0) {
    draft.detections.push(
      "deprecated ptr mechanism in SPF record (spf_ptr_mechanism)",
    );
  }

  // Go: `if !response.Valid && response.DNSLookups <= 10 { return }` -- when
  // the record is syntactically invalid the strength checks are skipped
  // (the parse is not trustworthy), unless invalidity came from the lookup
  // count alone.
  if (!analysis.valid && analysis.dnsLookups <= 10) return draft;

  if (analysis.allQualifier === "+" || record.includes("+all")) {
    draft.detections.push(
      "SPF record ends in +all and authorizes any server (spf_allows_all)",
    );
  }
  if (analysis.allQualifier === "~" || record.includes("~all")) {
    draft.detections.push(
      "SPF record uses softfail ~all (spf_weak)",
    );
  }
  // NOTE: `?all` maps to Go's "neutral" bucket, which buildIssues does NOT
  // flag -- kept identical here even though neutral is arguably weaker.

  if (analysis.dnsLookups > 10) {
    draft.detections.push(
      `SPF requires ${analysis.dnsLookups} DNS lookups, over the RFC limit of 10 (spf_lookups_critical)`,
    );
  } else if (analysis.dnsLookups === 10) {
    draft.detections.push(
      "SPF requires exactly 10 DNS lookups, at the RFC limit (spf_lookups_excessive)",
    );
  } else if (analysis.dnsLookups >= 8) {
    draft.detections.push(
      `SPF requires ${analysis.dnsLookups} DNS lookups, close to the RFC limit of 10 (spf_lookups_high)`,
    );
  }
  return draft;
}

// ---------------------------------------------------------------------------
// DMARC record analysis (port of dmarc_scanner buildIssues).
//
// Ported rules: dmarc_missing, dmarc_weak (p=none), dmarc_weak_percent
// (pct<100), dmarc_quarantine, dmarc_no_reporting (no rua).
// Deliberately skipped: the no-mail branch (same operator-intent reason as
// SPF). The Go source has NO subdomain-policy (sp weaker than p) rule, so
// none is ported. Like Go, an unparsable record found at _dmarc skips the
// strength checks instead of failing them.

type DmarcTags = {
  policy: string;
  percentage: number;
  ruaCount: number;
  valid: boolean;
};

function parseDmarcTags(record: string): DmarcTags {
  const tags = new Map<string, string>();
  for (const part of record.split(";")) {
    const eq = part.indexOf("=");
    if (eq < 0) continue;
    tags.set(part.slice(0, eq).trim().toLowerCase(), part.slice(eq + 1).trim());
  }
  const policy = (tags.get("p") ?? "").toLowerCase();
  const pctRaw = tags.get("pct");
  let percentage = 100;
  if (pctRaw !== undefined) {
    const parsed = Number.parseInt(pctRaw, 10);
    if (Number.isFinite(parsed)) percentage = parsed;
  }
  const rua = tags.get("rua") ?? "";
  const ruaCount = rua === "" ? 0 : rua.split(",").filter((u) => u.trim() !== "").length;
  const valid =
    (tags.get("v") ?? "").toUpperCase() === "DMARC1" &&
    ["none", "quarantine", "reject"].includes(policy);
  return { policy, percentage, ruaCount, valid };
}

function buildDmarcControl(dmarcRecords: string[]): ControlDraft {
  const draft: ControlDraft = {
    key: "dmarc",
    label: "DMARC",
    present: dmarcRecords.length > 0,
    detections: [],
    reasons: [],
    evidence: [...dmarcRecords],
  };
  if (dmarcRecords.length === 0) {
    draft.detections.push("no DMARC record found (dmarc_missing)");
    return draft;
  }
  const tags = parseDmarcTags(dmarcRecords[0]!);
  if (!tags.valid) {
    draft.reasons.push(
      "DMARC record present but not parseable; strength checks skipped",
    );
    return draft;
  }
  if (tags.policy === "none") {
    draft.detections.push("DMARC policy is p=none, monitoring only (dmarc_weak)");
  }
  if (tags.percentage < 100) {
    draft.detections.push(
      `DMARC applies to only ${tags.percentage}% of mail (dmarc_weak_percent)`,
    );
  }
  if (tags.policy === "quarantine") {
    draft.detections.push(
      "DMARC policy is p=quarantine, weaker than reject (dmarc_quarantine)",
    );
  }
  if (tags.ruaCount === 0) {
    draft.detections.push(
      "no rua= aggregate reporting address (dmarc_no_reporting)",
    );
  }
  return draft;
}

// ---------------------------------------------------------------------------
// MX record analysis (port of mx_scanner buildIssues).
//
// Ported rules: mx_missing, mx_localhost. Additionally a null MX target
// (RFC 7505, `0 .`) is recognized: runner3 would count "." as a host, but
// here it is treated as "no usable MX hosts" with an explanatory reason,
// since a null MX explicitly declares the domain accepts no mail.
// Deliberately skipped (live network required): mx_no_tls (needs an SMTP
// STARTTLS probe), mx_rfc1918 / mx_special_ip (need the MX hostnames
// resolved to addresses -- the crawl rows for the MX targets may live under
// a different root_domain entirely).

function buildMxControl(
  mxValues: string[],
): { draft: ControlDraft; hostCount: number } {
  const draft: ControlDraft = {
    key: "mx",
    label: "MX",
    present: false,
    detections: [],
    reasons: [],
    evidence: [...mxValues],
  };
  const hosts: string[] = [];
  let nullMx = false;
  for (const value of mxValues) {
    // Values look like `10 mx01.handelsbanken.se.`.
    const match = value.trim().match(/^(\d+)\s+(\S*)$/);
    const target = canonicalName(match ? match[2]! : value);
    if (target === "" || target === ".") {
      nullMx = true;
      continue;
    }
    hosts.push(target);
    if (target === "localhost") {
      draft.detections.push(`MX host is localhost: ${value.trim()} (mx_localhost)`);
    }
  }
  draft.present = hosts.length > 0;
  if (nullMx && hosts.length === 0) {
    draft.reasons.push(
      "null MX published (RFC 7505): the domain declares it does not accept mail",
    );
  } else if (mxValues.length === 0) {
    draft.detections.push("no MX records found (mx_missing)");
  } else if (hosts.length === 1) {
    draft.reasons.push("single MX host observed (no redundancy)");
  }
  return { draft, hostCount: hosts.length };
}

// ---------------------------------------------------------------------------

/**
 * Evaluate mail security for one domain from its crawled DNS records.
 *
 * Controls: mx, spf, dkim, dmarc, mta_sts, bimi. Runner3's DANE control is
 * NOT ported (owner 2026-08-29: no DANE testing here) and its
 * "dns" step control has no crawl
 * equivalent and is dropped; DNSSEC presence feeds the summary exactly like
 * runner3's snapshotHasDNSSEC feeds its summary. Scoring follows Go
 * deriveOutput: start at 100, warn -10, fail -20, unknown -15, floor 0
 * (the FailedSteps*5 penalty has no equivalent here). mail_ready =
 * MX + SPF + DMARC present.
 */
export function evaluateMailSecurity(
  records: MailDnsRecord[],
  domain: string,
): MailSecurityReport {
  const apex = canonicalName(domain);
  const usedRecords: MailDnsRecord[] = [];
  const use = <T extends MailDnsRecord>(record: T): T => {
    usedRecords.push(record);
    return record;
  };

  type Normalized = MailDnsRecord & { canonicalName: string; text: string };
  const normalized: Normalized[] = records.map((record) => ({
    ...record,
    canonicalName: canonicalName(record.name),
    text: normalizeTxtValue(record.value),
  }));

  const dedupe = (rows: Normalized[]): Normalized[] => {
    const seen = new Set<string>();
    const result: Normalized[] = [];
    for (const row of rows) {
      const key = `${row.canonicalName}\u0000${row.text}`;
      if (seen.has(key)) continue;
      seen.add(key);
      result.push(row);
    }
    return result;
  };

  // --- MX ---------------------------------------------------------------
  const mxRows = dedupe(
    normalized.filter(
      (r) => r.record_type === "MX" && r.canonicalName === apex,
    ),
  ).map(use);
  const mx = buildMxControl(mxRows.map((r) => r.text));

  // --- SPF ----------------------------------------------------------------
  const isSpfText = (text: string) => /^v=spf1(\s|$)/i.test(text.trim());
  const spfTxtRows = dedupe(
    normalized.filter(
      (r) =>
        r.record_type === "TXT" && r.canonicalName === apex && isSpfText(r.text),
    ),
  ).map(use);
  // Legacy `SPF` record type (rare): counts as presence only.
  const legacySpfRows = dedupe(
    normalized.filter(
      (r) =>
        r.record_type === "SPF" && r.canonicalName === apex && isSpfText(r.text),
    ),
  ).map(use);
  const spf = buildSpfControl(
    spfTxtRows.map((r) => r.text),
    legacySpfRows.length,
  );
  if (spfTxtRows.length > 0 && legacySpfRows.length > 0) {
    spf.reasons.push(
      "a deprecated SPF-type record also exists; the TXT record is canonical",
    );
    spf.evidence.push(...legacySpfRows.map((r) => r.text));
  }

  // --- DMARC --------------------------------------------------------------
  const dmarcRows = dedupe(
    normalized.filter(
      (r) =>
        r.record_type === "TXT" &&
        r.canonicalName === `_dmarc.${apex}` &&
        /^v=dmarc1(\s*;|\s|$)/i.test(r.text.trim()),
    ),
  ).map(use);
  const dmarc = buildDmarcControl(dmarcRows.map((r) => r.text));

  // --- DKIM ---------------------------------------------------------------
  // Presence = any `<selector>._domainkey.<...>` TXT (or a CNAME delegating
  // a selector to a provider) observed in the crawl. Crawlers only see the
  // selectors they happened to query, so NO observed selectors is `unknown`,
  // never `fail` -- absence of evidence here is not evidence of absence.
  const dkimRows = dedupe(
    normalized.filter(
      (r) =>
        (r.record_type === "TXT" || r.record_type === "CNAME") &&
        r.canonicalName.includes("._domainkey."),
    ),
  ).map(use);
  const dkimSelectors = [
    ...new Set(
      dkimRows.map((r) => r.canonicalName.split("._domainkey.")[0] ?? ""),
    ),
  ].filter((s) => s !== "");
  const dkim: ControlDraft = {
    key: "dkim",
    label: "DKIM",
    present: dkimRows.length > 0,
    detections: [],
    reasons:
      dkimRows.length > 0
        ? [
            `${dkimRows.length} DKIM record(s) observed for selector(s): ${dkimSelectors.join(", ")}`,
          ]
        : ["no DKIM selectors observed in crawl (crawlers only see queried selectors)"],
    evidence: dkimRows.map((r) => `${r.canonicalName} ${r.text}`),
    forcedStatus: dkimRows.length > 0 ? undefined : "unknown",
  };

  // --- MTA-STS ------------------------------------------------------------
  // Record-present rules only: runner3 additionally FETCHES the policy file
  // over HTTPS and judges mode/max_age/mx-mismatch (mta_sts_mode_testing,
  // mta_sts_mode_none, mta_sts_weak_config, mta_sts_weak) -- all of that is
  // live network and is skipped. This control therefore makes the weaker
  // claim "an MTA-STS DNS record is present", not "a valid enforced policy
  // is served".
  const mtaStsRows = dedupe(
    normalized.filter(
      (r) =>
        r.record_type === "TXT" &&
        r.canonicalName === `_mta-sts.${apex}` &&
        /^v=stsv1(\s*;|\s|$)/i.test(r.text.trim()),
    ),
  ).map(use);
  const mtaSts: ControlDraft = {
    key: "mta_sts",
    label: "MTA-STS",
    present: mtaStsRows.length > 0,
    detections:
      mtaStsRows.length > 0
        ? []
        : ["no MTA-STS DNS record found (mta_sts_missing)"],
    reasons:
      mtaStsRows.length > 0
        ? [
            "MTA-STS DNS record present; the policy file itself is not fetched from crawl data (weaker claim than a live probe)",
          ]
        : [],
    evidence: mtaStsRows.map((r) => r.text),
  };

  // --- BIMI ---------------------------------------------------------------
  // Presence-level rule only (bimi_missing): any `<selector>._bimi.<domain>`
  // TXT with v=BIMI1. Runner3's logo/VMC URL checks are warnings there, not
  // detections, and are not ported.
  const bimiRows = dedupe(
    normalized.filter(
      (r) =>
        r.record_type === "TXT" &&
        r.canonicalName.endsWith(`._bimi.${apex}`) &&
        /^v=bimi1(\s*;|\s|$)/i.test(r.text.trim()),
    ),
  ).map(use);
  const bimi: ControlDraft = {
    key: "bimi",
    label: "BIMI",
    present: bimiRows.length > 0,
    detections:
      bimiRows.length > 0 ? [] : ["no BIMI record found (bimi_missing)"],
    reasons:
      bimiRows.length > 0
        ? [
            `BIMI record present for selector(s): ${[
              ...new Set(
                bimiRows.map(
                  (r) => r.canonicalName.split("._bimi.")[0] ?? "default",
                ),
              ),
            ].join(", ")}`,
          ]
        : [],
    evidence: bimiRows.map((r) => r.text),
  };

  // --- DNSSEC (summary only, like runner3's snapshotHasDNSSEC) ------------
  const dnssecRows = normalized
    .filter((r) => ["RRSIG", "DNSKEY", "DS"].includes(r.record_type))
    .map(use);
  const dnssecAvailable = dnssecRows.length > 0;

  const drafts = [mx.draft, spf, dkim, dmarc, mtaSts, bimi];
  const controls = drafts.map(finalizeControl);

  // Port of Go deriveOutput scoring (minus FailedSteps: no live steps).
  // Owner 2026-08-29: an UNOBSERVED DKIM control is a crawl-coverage gap, not
  // a domain deficiency -- it is shown but excluded from the score, and the
  // report says so (scored_without_dkim).
  const scoredWithoutDkim = dkimRows.length === 0;
  let score = 100;
  for (const control of controls) {
    if (scoredWithoutDkim && control.key === "dkim") continue;
    if (control.status === "warn") score -= 10;
    else if (control.status === "fail") score -= 20;
    else if (control.status === "unknown") score -= 15;
  }
  if (score < 0) score = 0;

  const summary: MailSecuritySummary = {
    mx_found: mx.draft.present,
    mx_host_count: mx.hostCount,
    spf_found: spf.present,
    dmarc_found: dmarc.present,
    dkim_records_found: dkimRows.length,
    mta_sts_found: mtaSts.present,
    bimi_found: bimi.present,
    dnssec_available: dnssecAvailable,
    total_detections: drafts.reduce((n, d) => n + d.detections.length, 0),
  };

  let lastSeen = "";
  for (const record of usedRecords) {
    if (record.last_seen !== undefined && record.last_seen > lastSeen) {
      lastSeen = record.last_seen;
    }
  }

  return {
    domain: apex,
    score,
    mail_ready: summary.mx_found && summary.spf_found && summary.dmarc_found,
    summary,
    controls,
    last_seen: lastSeen,
    scored_without_dkim: scoredWithoutDkim,
  };
}
