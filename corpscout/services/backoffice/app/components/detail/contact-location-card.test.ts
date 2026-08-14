import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  AddressGeocodeOutcomeNotice,
  AddressSourceEvidence,
  addressGeocodeEvidenceLink,
  addressGeocodeOutcomeCopy,
  foreignAddressBadgeText,
  geocodeCountryCodeForAddress,
  storedAddressGeocode,
} from "~/components/detail/contact-location-card";
import type { AddressRow } from "~/lib/queries.server";

const polishAddress: AddressRow = {
  address_type: "visiting_or_postal",
  full_address: "UL. GLINKI 146 PL BYDGOSZCZ, 00000 Utlandet",
  geocode_address: "GLINKI 146, BYDGOSZCZ",
  address_country_code: "PL",
  address_is_foreign: 1,
};

describe("foreign address presentation", () => {
  it("shows the known address country and scopes geocoding to it", () => {
    expect(foreignAddressBadgeText(polishAddress, "Sweden")).toBe(
      "🇵🇱 Address in Poland",
    );
    expect(geocodeCountryCodeForAddress(polishAddress, "se")).toBe("pl");
  });

  it("marks an unknown foreign country without incorrectly using Sweden", () => {
    const unknownForeignAddress: AddressRow = {
      address_type: "visiting_or_postal",
      full_address: "7 BELL YARD LONDON WC2A 2JR, 00000 Utlandet",
      address_is_foreign: 1,
    };

    expect(foreignAddressBadgeText(unknownForeignAddress, "Sweden")).toBe(
      "🌐 Address outside Sweden",
    );
    expect(geocodeCountryCodeForAddress(unknownForeignAddress, "se")).toBe("");
  });

  it("keeps domestic addresses scoped to their register country", () => {
    const swedishAddress: AddressRow = {
      address_type: "postal",
      full_address: "PRÄSTGÅRDSLIDEN 4 C, 59542 MJÖLBY",
      address_country_code: "SE",
      address_is_foreign: 0,
    };

    expect(foreignAddressBadgeText(swedishAddress, "Sweden")).toBeNull();
    expect(geocodeCountryCodeForAddress(swedishAddress, "se")).toBe("se");
  });
});

describe("stored address geocode evidence", () => {
  it("returns the coordinates and OSM provenance from the matched address", () => {
    const geocode = storedAddressGeocode({}, [
      {
        address_type: "postal",
        full_address: "Storgatan 10 A, 111 22 Stockholm",
        latitude: 59.331,
        longitude: 18.061,
        geocode_status: "matched_exact",
        geocode_provider: "openstreetmap",
        geocode_precision: "building",
        geocode_match_method: "postal_code_street_house_exact_unique",
        geocode_match_confidence: 1,
        geocode_source_record_url: "https://www.openstreetmap.org/way/100",
        geocode_source_snapshot_at: "2026-08-11T23:11:37.000Z",
      },
    ]);

    expect(geocode).toMatchObject({
      lat: 59.331,
      lon: 18.061,
      provider: "openstreetmap",
      sourceRecordUrl: "https://www.openstreetmap.org/way/100",
      sourceSnapshotAt: "2026-08-11T23:11:37.000Z",
    });
  });

  it("returns an explicitly approximate city fallback for a PO box", () => {
    const geocode = storedAddressGeocode({}, [
      {
        address_type: "postal",
        full_address: "Box 222, 147 01 Tumba",
        latitude: 59.2027552,
        longitude: 17.8307813,
        geocode_status: "postal_box",
        geocode_provider: "openstreetmap",
        geocode_precision: "city",
        geocode_coordinate_locality: "Tumba",
        geocode_coordinate_supporting_point_count: 782,
      },
    ]);

    expect(geocode).toMatchObject({
      lat: 59.2027552,
      lon: 17.8307813,
      provider: "openstreetmap",
      precision: "city",
      coordinateLocality: "Tumba",
      coordinateSupportingPointCount: 782,
    });
  });
});

describe("address geocoding outcome explanations", () => {
  it("explains an exact city-address fallback", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "visiting_or_postal",
        full_address: "Transportgatan 11, 26271 ÄNGELHOLM",
        geocode_status: "matched_exact",
        geocode_match_method: "city_street_house_exact_unique",
      })?.description,
    ).toBe(
      "City, street, and house number matched one OpenStreetMap record.",
    );
  });

  it("links exact coordinates to the matched OSM record", () => {
    expect(
      addressGeocodeEvidenceLink({
        address_type: "visiting",
        full_address: "Storgatan 10, 111 22 Stockholm",
        geocode_source_record_url: "https://www.openstreetmap.org/way/100",
        geocode_source_url:
          "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
      }),
    ).toEqual({
      url: "https://www.openstreetmap.org/way/100",
      label: "Matched OpenStreetMap record",
    });
  });

  it("links approximate city coordinates to their OSM extract", () => {
    expect(
      addressGeocodeEvidenceLink({
        address_type: "postal",
        full_address: "Box 222, 147 01 Tumba",
        geocode_source_url:
          "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
      }),
    ).toEqual({
      url: "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
      label: "OpenStreetMap extract provenance",
    });
  });

  it("explains that a PO box has no exact physical location", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "postal",
        full_address: "Box 222, 147 01 Tumba",
        geocode_status: "postal_box",
      }),
    ).toEqual({
      title: "Postal address only",
      description:
        "This registry address is a PO box, so it does not identify a physical building.",
      badge: "PO box",
    });
  });

  it("explains when a PO box uses an approximate city map", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "postal",
        full_address: "Box 222, 147 01 Tumba",
        geocode_status: "postal_box",
        geocode_precision: "city",
        geocode_coordinate_locality: "Tumba",
      })?.description,
    ).toContain("The map shows an approximate location for Tumba");
  });

  it("includes the candidate count for ambiguous matches", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "visiting",
        full_address: "Drottninggatan 5, 111 51 Stockholm",
        geocode_status: "ambiguous",
        geocode_candidate_count: 2,
      })?.description,
    ).toContain("2 OpenStreetMap records");
  });

  it("shows every candidate link for an ambiguous match", () => {
    const candidateUrls = [
      "https://www.openstreetmap.org/node/400",
      "https://www.openstreetmap.org/node/401",
      "https://www.openstreetmap.org/node/402",
      "https://www.openstreetmap.org/node/403",
    ];
    const html = renderToStaticMarkup(
      createElement(AddressGeocodeOutcomeNotice, {
        address: {
          address_type: "visiting",
          full_address: "Hamngatan 7, 75320 UPPSALA",
          geocode_status: "ambiguous",
          geocode_candidate_count: candidateUrls.length,
          geocode_candidate_record_urls: candidateUrls,
        },
      }),
    );

    expect(html).toContain("Candidate 4");
    for (const url of candidateUrls) expect(html).toContain(url);
  });

  it("does not claim an outcome before an address has been processed", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "postal",
        full_address: "Storgatan 1, Stockholm",
      }),
    ).toBeNull();
  });
});

describe("canonical address source evidence", () => {
  it("keeps both source records under one canonical address", () => {
    const html = renderToStaticMarkup(
      createElement(AddressSourceEvidence, {
        members: [
          {
            address_key: "a".repeat(64),
            address_type: "postal",
            address_source: "bolagsverket",
            raw_address: "Box 222$TUMBA$14701",
            display_address: "Box 222, 14701 TUMBA",
            registry_source_record_uid: "bolagsverket-record",
            registry_source_run_id: "source-run",
            source_observed_at: "2026-08-12 19:00:00.000",
          },
          {
            address_key: "b".repeat(64),
            address_type: "visiting_or_postal",
            address_source: "scb",
            raw_address: "BOX 222, 14701 TUMBA",
            display_address: "BOX 222, 14701 TUMBA",
            registry_source_record_uid: "scb-record",
            registry_source_run_id: "source-run",
            source_observed_at: "2026-08-12 19:00:00.000",
          },
        ],
      }),
    );

    expect(html).toContain("Source records (2)");
  });
});
