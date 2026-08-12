import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { TechnologyIpAddressDetail } from "~/components/detail/technology-ip-address-detail";
import type { CompanyTechnologyIpDetail } from "~/lib/queries.server";

const detail: CompanyTechnologyIpDetail = {
  companyDomain: "example.se",
  companyHostnames: ["api.example.se"],
  historyIndexCoverage: {
    completedPartitions: 16,
    totalPartitions: 16,
  },
  address: {
    ip: "192.0.2.10",
    version: 4,
    networkSegment: "192.0.2.0/24",
    firstSeen: "2026-01-01 00:00:00.000",
    lastSeen: "2026-08-01 00:00:00.000",
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
      lastChangedAt: "2026-07-01 00:00:00.000",
      fetchedAt: "2026-08-01 00:00:00.000",
      sourceUrl: "https://rdap.example/ip/192.0.2.0",
    },
  },
  exactConnections: {
    page: 1,
    pageSize: 25,
    total: 1,
    hasMore: false,
    connections: [
      {
        ip: "192.0.2.10",
        version: 4,
        domain: "example.se",
        hostnames: ["api.example.se"],
        sources: ["authoritative"],
        discoveries: ["ct"],
        firstSeen: "2026-01-01 00:00:00.000",
        lastSeen: "2026-08-01 00:00:00.000",
      },
    ],
  },
  segmentConnections: {
    page: 1,
    pageSize: 25,
    total: null,
    hasMore: false,
    connections: [
      {
        ip: "192.0.2.20",
        version: 4,
        domain: "unrelated.example",
        hostnames: ["www.unrelated.example"],
        sources: ["authoritative"],
        discoveries: ["static"],
        firstSeen: "2026-02-01 00:00:00.000",
        lastSeen: "2026-07-01 00:00:00.000",
      },
    ],
  },
};

describe("TechnologyIpAddressDetail", () => {
  it("separates exact-IP evidence from the weaker network-segment signal", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter
        initialEntries={["/company/se/1/technology/ip-addresses/192.0.2.10"]}
      >
        <TechnologyIpAddressDetail
          detail={detail}
          companyContext={{
            domain: detail.companyDomain,
            hostnames: detail.companyHostnames,
          }}
          backLink={{ label: "All IP addresses", to: "..", relative: "path" }}
        />
      </MemoryRouter>,
    );

    expect(html).toContain("Domains on this exact IP");
    expect(html).toContain("This company");
    expect(html).toContain("Other domains in the network segment");
    expect(html).toContain("weaker than sharing the exact IP");
    expect(html).toContain("192.0.2.20");
    expect(html).toContain('href="/ip/192.0.2.20"');
    expect(html).toContain("unrelated.example");
    expect(html).toContain("192.0.2.0/24");
  });

  it("renders the canonical address view without company ownership context", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/ip/192.0.2.10"]}>
        <TechnologyIpAddressDetail detail={detail} />
      </MemoryRouter>,
    );

    expect(html).toContain("Observed across 1 domain connection");
    expect(html).toContain("DNS observation");
    expect(html).not.toContain("This company");
    expect(html).not.toContain("All IP addresses");
  });
});
