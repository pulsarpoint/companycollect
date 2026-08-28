import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  MailSecurityNoRecords,
  MailSecuritySection,
} from "~/components/detail/mail-security-section";
import { evaluateMailSecurity } from "~/lib/mail-security";

const report = evaluateMailSecurity(
  [
    {
      name: "handelsbanken.se",
      record_type: "MX",
      value: "10 mx01.handelsbanken.se.",
      last_seen: "2026-08-27 17:05:12.424",
    },
    {
      name: "handelsbanken.se",
      record_type: "MX",
      value: "10 mx02.handelsbanken.se.",
      last_seen: "2026-08-27 17:05:12.424",
    },
    {
      name: "handelsbanken.se",
      record_type: "TXT",
      value: '"v=spf1 include:spf.protection.outlook.com -all"',
      last_seen: "2026-08-27 17:05:12.424",
    },
    {
      name: "_dmarc.handelsbanken.se",
      record_type: "TXT",
      value: '"v=DMARC1; p=reject; rua=mailto:d@rua.example.com"',
      last_seen: "2026-08-27 17:05:12.424",
    },
    {
      name: "handelsbanken.se",
      record_type: "DNSKEY",
      value: "257 3 13 abc",
      last_seen: "2026-08-27 17:05:12.424",
    },
  ],
  "handelsbanken.se",
);

describe("MailSecuritySection", () => {
  const html = renderToStaticMarkup(<MailSecuritySection report={report} />);

  it("renders the score card with the big number and mail-ready badge", () => {
    expect(html).toContain(String(report.score));
    expect(html).toContain("/100");
    expect(html).toContain("Mail ready");
    expect(html).toContain("as of");
    expect(html).toContain("27 Aug 2026");
  });

  it("says the score comes from crawled DNS, not a live probe", () => {
    expect(html).toContain("crawled DNS records");
    expect(html).toContain("not a live probe");
  });

  it("renders every control with its status badge", () => {
    for (const label of [
      "MX",
      "SPF",
      "DKIM",
      "DMARC",
      "MTA-STS",
      "BIMI",
      "DANE TLSA",
    ]) {
      expect(html).toContain(label);
    }
    expect(html).toContain("pass");
    expect(html).toContain("fail");
    expect(html).toContain("unknown");
  });

  it("renders reasons and monospace evidence records", () => {
    expect(html).toContain("capability present and evaluated");
    expect(html).toContain("v=spf1 include:spf.protection.outlook.com -all");
    expect(html).toContain("10 mx01.handelsbanken.se.");
    expect(html).toContain("font-mono");
  });

  it("shows the not-mail-ready badge when a pillar is missing", () => {
    const failing = evaluateMailSecurity([], "example.se");
    const failingHtml = renderToStaticMarkup(
      <MailSecuritySection report={failing} />,
    );
    expect(failingHtml).toContain("Not mail ready");
    expect(failingHtml).toContain(">0<");
  });
});

describe("MailSecurityNoRecords", () => {
  it("renders the empty state naming the domain", () => {
    const html = renderToStaticMarkup(
      <MailSecurityNoRecords domain="example.se" />,
    );
    expect(html).toContain("No DNS records held for this domain");
    expect(html).toContain("example.se");
  });
});
