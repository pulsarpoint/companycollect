import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { TechnologyInfrastructureSection } from "~/components/detail/technology-infrastructure-section";
import type { CompanyTechnologyInfrastructure } from "~/lib/queries.server";

const infrastructure: CompanyTechnologyInfrastructure = {
  domain: "example.se",
  page: 1,
  pageSize: 25,
  summary: {
    totalHostnames: 10,
    certificateHostnames: 8,
    dnsHostnames: 9,
    resolvedIpAddressesOnPage: 3,
    rdapRegisteredIpAddressesOnPage: 1,
  },
  scan: {
    status: "done",
    resolvedAt: "2026-08-06 10:00:00.000",
    nameservers: ["ns1.example.se"],
    nameserverIps: ["192.0.2.53"],
    queriesTotal: 10,
    queriesOk: 10,
    dnssecSigned: true,
    dsOutcome: "present",
    dnskeyOutcome: "present",
    zoneTransferOpen: false,
  },
  hostnames: [
    {
      hostname: "api.example.se",
      label: "api",
      isApex: false,
      isWildcard: false,
      evidence: ["certificate", "dns"],
      certificateFirstSeen: "2026-01-01 00:00:00.000",
      certificateLastSeen: "2026-07-01 00:00:00.000",
      certificateExpiresAt: "2026-10-01 00:00:00",
      certificateSourceLogs: ["argon2026h1"],
      dnsFirstSeen: "2026-01-02 00:00:00.000",
      dnsLastSeen: "2026-08-06 10:00:00.000",
      dnsDiscoverySource: "ct",
      hasIpv4: true,
      hasIpv6: false,
      hasCname: false,
      records: [
        {
          type: "A",
          value: "192.0.2.10",
          priority: 0,
          sources: ["authoritative"],
          discoveries: ["ct"],
          seenDates: ["2026-08-06"],
          firstSeen: "2026-01-02 00:00:00.000",
          lastSeen: "2026-08-06 10:00:00.000",
        },
      ],
      ipAddresses: [
        {
          ip: "192.0.2.10",
          version: 4,
          networkSegment: "192.0.2.0/24",
          firstSeen: "2026-01-02 00:00:00.000",
          lastSeen: "2026-08-06 10:00:00.000",
          countryCode: "SE",
          countryName: "Sweden",
          cityName: "Stockholm",
          asn: 64500,
          asnOrganization: "Example Network",
          rdapRegistration: {
            networkKey: "arin:NET-192-0-2-0-1",
            matchedCidr: "192.0.2.0/24",
            rir: "arin",
            handle: "NET-192-0-2-0-1",
            name: "TEST-NET-1",
            registrationType: "DIRECT ALLOCATION",
            countryCode: "US",
            statuses: ["active"],
            registrantNames: ["Example Corporation"],
            startAddress: "192.0.2.0",
            endAddress: "192.0.2.255",
            registrationDate: "2010-01-01 00:00:00.000",
            lastChangedAt: "2026-07-01 12:00:00.000",
            fetchedAt: "2026-08-06 12:00:00.000",
            sourceUrl: "https://rdap.arin.net/registry/ip/192.0.2.0",
          },
        },
      ],
    },
  ],
};

describe("TechnologyInfrastructureSection", () => {
  it("shows hostname evidence, DNS history, and enriched IP addresses", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter
        initialEntries={["/company/se/5594643297/technology/infrastructure"]}
      >
        <TechnologyInfrastructureSection
          infrastructure={infrastructure}
          ipAddressesPath="/company/se/5594643297/technology/ip-addresses"
        />
      </MemoryRouter>,
    );

    expect(html).toContain("10 observed hostnames");
    expect(html).toContain("Certificate log");
    expect(html).toContain("DNS-confirmed");
    expect(html).toContain("api.example.se");
    expect(html).toContain("192.0.2.10");
    expect(html).toContain("Example Network");
    expect(html).toContain("1 with RDAP registration");
    expect(html).toContain("RDAP registration");
    expect(html).toContain("Example Corporation");
    expect(html).toContain("TEST-NET-1");
    expect(html).toContain("192.0.2.0/24");
    expect(html).toContain(
      'href="/company/se/5594643297/technology/ip-addresses/192.0.2.10?domain=example.se"',
    );
    expect(html).toContain("Registry record");
    expect(html).toContain("First observed");
    expect(html).toContain("Last observed");
  });
});
