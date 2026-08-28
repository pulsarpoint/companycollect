import { describe, expect, it } from "vitest";
import {
  evaluateMailSecurity,
  normalizeTxtValue,
  type MailDnsRecord,
} from "~/lib/mail-security";

function record(
  name: string,
  record_type: string,
  value: string,
  last_seen?: string,
): MailDnsRecord {
  return { name, record_type, value, last_seen };
}

function control(
  report: ReturnType<typeof evaluateMailSecurity>,
  key: string,
) {
  const found = report.controls.find((c) => c.key === key);
  if (!found) throw new Error(`control ${key} missing`);
  return found;
}

/** Handelsbanken-like near-perfect fixture: DMARC p=reject with rua, strict
 * SPF -all, two MX hosts, MTA-STS record, BIMI, DNSSEC (DNSKEY/DS/RRSIG).
 * TXT values quoted, exactly as stored in ClickHouse. */
function handelsbankenLike(): MailDnsRecord[] {
  return [
    record("handelsbanken.se", "MX", "10 mx01.handelsbanken.se.", "2026-08-27 17:05:12.424"),
    record("handelsbanken.se", "MX", "10 mx02.handelsbanken.se.", "2026-08-27 17:05:12.424"),
    record(
      "handelsbanken.se",
      "TXT",
      '"v=spf1 ip4:194.68.56.145 include:spf.protection.outlook.com -all"',
      "2026-08-27 17:05:12.424",
    ),
    record(
      "_dmarc.handelsbanken.se",
      "TXT",
      '"v=DMARC1; p=reject; rua=mailto:handelsbanken@rua.netcraft.com; ruf=mailto:handelsbanken@ruf.netcraft.com; fo=1;"',
      "2026-08-27 17:05:12.424",
    ),
    record("_mta-sts.handelsbanken.se", "TXT", '"v=STSv1; id=20200505080000Z"'),
    record(
      "default._bimi.handelsbanken.se",
      "TXT",
      '"v=BIMI1; l=https://www.handelsbanken.se/sv/sepu/shb.svg; a=https://www.handelsbanken.se/sv/sepu/shb.pem"',
    ),
    record("handelsbanken.se", "DNSKEY", "257 3 13 nyz0eHz...", "2026-08-27 17:05:12.424"),
    record("handelsbanken.se", "DS", "7418 13 2 0BA401AE...", "2026-08-27 17:05:12.424"),
    record("handelsbanken.se", "RRSIG", "MX 13 2 3600 ...", "2026-08-27 17:05:12.424"),
  ];
}

describe("normalizeTxtValue", () => {
  it("strips surrounding double quotes", () => {
    expect(normalizeTxtValue('"v=DMARC1; p=reject;"')).toBe(
      "v=DMARC1; p=reject;",
    );
  });

  it("passes unquoted values through", () => {
    expect(normalizeTxtValue("v=STSv1; id=20200505080000Z")).toBe(
      "v=STSv1; id=20200505080000Z",
    );
  });

  it("joins multi-chunk quoted records per RFC 1035 concatenation", () => {
    expect(
      normalizeTxtValue('"v=spf1 include:_spf.example.com" " ip4:192.0.2.1 -all"'),
    ).toBe("v=spf1 include:_spf.example.com ip4:192.0.2.1 -all");
  });

  it("unescapes backslash-escaped characters inside chunks", () => {
    expect(normalizeTxtValue('"a\\"b"')).toBe('a"b');
  });
});

describe("evaluateMailSecurity", () => {
  it("scores the Handelsbanken-like near-perfect case high", () => {
    const report = evaluateMailSecurity(handelsbankenLike(), "handelsbanken.se");
    expect(control(report, "mx").status).toBe("pass");
    expect(control(report, "spf").status).toBe("pass");
    expect(control(report, "dmarc").status).toBe("pass");
    expect(control(report, "mta_sts").status).toBe("pass");
    expect(control(report, "bimi").status).toBe("pass");
    // DKIM selectors are unobservable in this crawl slice: shown as
    // `unknown` but EXCLUDED from the score (owner 2026-08-29), so the
    // well-configured fixture reaches the full 100.
    expect(control(report, "dkim").status).toBe("unknown");
    expect(report.scored_without_dkim).toBe(true);
    expect(report.score).toBe(100);
    expect(report.mail_ready).toBe(true);
    expect(report.summary.dnssec_available).toBe(true);
    expect(report.summary.mx_host_count).toBe(2);
    expect(report.last_seen).toBe("2026-08-27 17:05:12.424");
  });

  it("marks SPF ~all as a warn-level detection (spf_weak)", () => {
    const records = handelsbankenLike().map((r) =>
      r.value.includes("v=spf1") ? { ...r, value: '"v=spf1 include:spf.protection.outlook.com ~all"' } : r,
    );
    const report = evaluateMailSecurity(records, "handelsbanken.se");
    const spf = control(report, "spf");
    expect(spf.status).toBe("warn");
    expect(spf.reasons.join(" ")).toContain("spf_weak");
    expect(report.score).toBe(90);
  });

  it("marks DMARC p=none as warn and keeps mail_ready", () => {
    const report = evaluateMailSecurity(
      [
        record("example.se", "MX", "10 mail.example.se."),
        record("example.se", "TXT", '"v=spf1 -all"'),
        record("_dmarc.example.se", "TXT", '"v=DMARC1; p=none; rua=mailto:d@example.se"'),
      ],
      "example.se",
    );
    const dmarc = control(report, "dmarc");
    expect(dmarc.status).toBe("warn");
    expect(dmarc.reasons.join(" ")).toContain("dmarc_weak");
    expect(report.mail_ready).toBe(true);
  });

  it("flags pct<100, quarantine, and missing rua as separate detections", () => {
    const report = evaluateMailSecurity(
      [record("_dmarc.example.se", "TXT", '"v=DMARC1; p=quarantine; pct=50"')],
      "example.se",
    );
    const dmarc = control(report, "dmarc");
    expect(dmarc.status).toBe("warn");
    const text = dmarc.reasons.join(" ");
    expect(text).toContain("dmarc_weak_percent");
    expect(text).toContain("dmarc_quarantine");
    expect(text).toContain("dmarc_no_reporting");
  });

  it("fails the DMARC control when the record is missing", () => {
    const report = evaluateMailSecurity(
      [
        record("example.se", "MX", "10 mail.example.se."),
        record("example.se", "TXT", '"v=spf1 -all"'),
      ],
      "example.se",
    );
    const dmarc = control(report, "dmarc");
    expect(dmarc.status).toBe("fail");
    expect(dmarc.present).toBe(false);
    expect(dmarc.reasons.join(" ")).toContain("dmarc_missing");
    expect(report.mail_ready).toBe(false);
  });

  it("detects +all as spf_allows_all", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "TXT", '"v=spf1 +all"')],
      "example.se",
    );
    const spf = control(report, "spf");
    expect(spf.status).toBe("warn");
    expect(spf.reasons.join(" ")).toContain("spf_allows_all");
  });

  it("detects multiple SPF records", () => {
    const report = evaluateMailSecurity(
      [
        record("example.se", "TXT", '"v=spf1 include:a.example.com -all"'),
        record("example.se", "TXT", '"v=spf1 include:b.example.com -all"'),
      ],
      "example.se",
    );
    expect(control(report, "spf").reasons.join(" ")).toContain(
      "spf_multiple_records",
    );
  });

  it("does not count quoted/unquoted duplicates of one record as multiple SPF records", () => {
    // The _current table holds the same record both quoted and unquoted.
    const report = evaluateMailSecurity(
      [
        record("example.se", "TXT", '"v=spf1 -all"'),
        record("example.se", "TXT", "v=spf1 -all"),
      ],
      "example.se",
    );
    const spf = control(report, "spf");
    expect(spf.status).toBe("pass");
    expect(spf.reasons.join(" ")).not.toContain("spf_multiple_records");
  });

  it("detects the deprecated ptr mechanism and syntax errors", () => {
    const ptr = evaluateMailSecurity(
      [record("example.se", "TXT", '"v=spf1 ptr -all"')],
      "example.se",
    );
    expect(control(ptr, "spf").reasons.join(" ")).toContain(
      "spf_ptr_mechanism",
    );

    const syntax = evaluateMailSecurity(
      [record("example.se", "TXT", '"v=spf1 bogusmech -all"')],
      "example.se",
    );
    expect(control(syntax, "spf").reasons.join(" ")).toContain(
      "spf_syntax_error",
    );
    // Invalid records skip the strength checks, like the Go scanner.
    expect(control(syntax, "spf").reasons.join(" ")).not.toContain("spf_weak");
  });

  it("flags duplicate all terminators and unreachable tokens", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "TXT", '"v=spf1 -all ip4:192.0.2.1 -all"')],
      "example.se",
    );
    expect(control(report, "spf").reasons.join(" ")).toContain(
      "spf_syntax_error",
    );
  });

  it("counts top-level SPF DNS lookups like the Go scanner", () => {
    const eight = evaluateMailSecurity(
      [
        record(
          "example.se",
          "TXT",
          '"v=spf1 include:a include:b include:c include:d include:e include:f a mx -all"',
        ),
      ],
      "example.se",
    );
    expect(control(eight, "spf").reasons.join(" ")).toContain(
      "spf_lookups_high",
    );

    const eleven = evaluateMailSecurity(
      [
        record(
          "example.se",
          "TXT",
          '"v=spf1 include:a include:b include:c include:d include:e include:f include:g include:h include:i a mx redirect=x.example.com"',
        ),
      ],
      "example.se",
    );
    expect(control(eleven, "spf").reasons.join(" ")).toContain(
      "spf_lookups_critical",
    );
  });

  it("fails the SPF control when no record exists", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "MX", "10 mail.example.se.")],
      "example.se",
    );
    const spf = control(report, "spf");
    expect(spf.status).toBe("fail");
    expect(spf.reasons.join(" ")).toContain("spf_missing");
  });

  it("treats a legacy SPF-type record as presence only", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "SPF", '"v=spf1 +all"')],
      "example.se",
    );
    const spf = control(report, "spf");
    expect(spf.present).toBe(true);
    expect(spf.status).toBe("pass");
    // No analysis of the deprecated type: +all is deliberately not flagged.
    expect(spf.reasons.join(" ")).toContain("deprecated SPF record type");
  });

  it("fails the MX control when no MX records exist", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "TXT", '"v=spf1 -all"')],
      "example.se",
    );
    const mx = control(report, "mx");
    expect(mx.status).toBe("fail");
    expect(mx.reasons.join(" ")).toContain("mx_missing");
    expect(report.mail_ready).toBe(false);
  });

  it("recognizes a null MX (RFC 7505) as not accepting mail", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "MX", "0 .")],
      "example.se",
    );
    const mx = control(report, "mx");
    expect(mx.present).toBe(false);
    expect(mx.reasons.join(" ")).toContain("RFC 7505");
    expect(report.summary.mx_host_count).toBe(0);
  });

  it("detects a localhost MX host", () => {
    const report = evaluateMailSecurity(
      [record("example.se", "MX", "10 localhost.")],
      "example.se",
    );
    const mx = control(report, "mx");
    expect(mx.status).toBe("warn");
    expect(mx.reasons.join(" ")).toContain("mx_localhost");
  });

  it("marks DKIM unknown when no selectors were observed, pass when they were", () => {
    const withoutDkim = evaluateMailSecurity([], "example.se");
    const dkimUnknown = control(withoutDkim, "dkim");
    expect(dkimUnknown.status).toBe("unknown");
    expect(dkimUnknown.reasons.join(" ")).toContain(
      "no DKIM selectors observed in crawl",
    );

    const withDkim = evaluateMailSecurity(
      handelsbankenLike().concat([
        record(
          "selector1._domainkey.handelsbanken.se",
          "TXT",
          '"v=DKIM1; k=rsa; p=MIGf..."',
        ),
        record(
          "selector2._domainkey.handelsbanken.se",
          "CNAME",
          "selector2.example.dkim.provider.com.",
        ),
      ]),
      "handelsbanken.se",
    );
    const dkim = control(withDkim, "dkim");
    expect(dkim.status).toBe("pass");
    expect(dkim.reasons.join(" ")).toContain("selector1");
    expect(dkim.reasons.join(" ")).toContain("selector2");
    // With selectors observed DKIM re-enters the score as a pass; the rich
    // fixture stays at the full 100 either way.
    expect(withDkim.scored_without_dkim).toBe(false);
    expect(withDkim.score).toBe(100);
  });

  it("treats the MTA-STS control as record-presence only", () => {
    const report = evaluateMailSecurity(
      [record("_mta-sts.example.se", "TXT", '"v=STSv1; id=2024"')],
      "example.se",
    );
    const mtaSts = control(report, "mta_sts");
    expect(mtaSts.status).toBe("pass");
    expect(mtaSts.reasons.join(" ")).toContain("policy file");
  });

  it("has no DANE control and excludes unobserved DKIM from the score", () => {
    const report = evaluateMailSecurity([], "example.se");
    expect(report.controls.map((c) => c.key)).not.toContain("dane_tlsa");
    // Unobserved DKIM is displayed as unknown but costs nothing.
    const dkim = control(report, "dkim");
    expect(dkim.status).toBe("unknown");
    expect(report.scored_without_dkim).toBe(true);
    // Empty domain: mx/spf/dmarc fail (-20 each), mta_sts/bimi fail... every
    // OTHER control contributes; dkim does not.
    const contributing = report.controls.filter((c) => c.key !== "dkim");
    const expected = Math.max(
      0,
      100 -
        contributing.filter((c) => c.status === "fail").length * 20 -
        contributing.filter((c) => c.status === "warn").length * 10 -
        contributing.filter((c) => c.status === "unknown").length * 15,
    );
    expect(report.score).toBe(expected);
  });

  it("keeps observed DKIM in the score", () => {
    const withDkim = evaluateMailSecurity(
      [record("s1._domainkey.example.se", "TXT", '"v=DKIM1; k=rsa; p=abc"')],
      "example.se",
    );
    expect(withDkim.scored_without_dkim).toBe(false);
    expect(control(withDkim, "dkim").status).toBe("pass");
  });

  it("parses split multi-chunk quoted TXT records", () => {
    const report = evaluateMailSecurity(
      [
        record(
          "example.se",
          "TXT",
          '"v=spf1 include:_spf.example.com" " ip4:192.0.2.1 -all"',
        ),
      ],
      "example.se",
    );
    const spf = control(report, "spf");
    expect(spf.status).toBe("pass");
    expect(spf.evidence[0]).toBe(
      "v=spf1 include:_spf.example.com ip4:192.0.2.1 -all",
    );
  });

  it("floors the score at 0 for an empty record set", () => {
    const report = evaluateMailSecurity([], "example.se");
    expect(report.score).toBe(0);
    expect(report.mail_ready).toBe(false);
    expect(report.last_seen).toBe("");
    expect(control(report, "mx").status).toBe("fail");
    expect(control(report, "spf").status).toBe("fail");
    expect(control(report, "dmarc").status).toBe("fail");
    expect(control(report, "mta_sts").status).toBe("fail");
    expect(control(report, "bimi").status).toBe("fail");
    expect(control(report, "dkim").status).toBe("unknown");
  });

  it("ignores records for other names and unrelated TXT content", () => {
    const report = evaluateMailSecurity(
      [
        record("other.example.se", "MX", "10 mail.example.se."),
        record("example.se", "TXT", '"google-site-verification=abc"'),
      ],
      "example.se",
    );
    expect(control(report, "mx").status).toBe("fail");
    expect(control(report, "spf").status).toBe("fail");
  });
});
