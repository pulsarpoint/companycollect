import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  AddressGeocodeOutcomeNotice,
  AddressSourceEvidence,
  addressGeocodeEvidenceLink,
  addressGeocodeOutcomeCopy,
  canRequestInteractiveGeocode,
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
    expect(canRequestInteractiveGeocode(polishAddress, "se")).toBe(true);
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
    expect(canRequestInteractiveGeocode(unknownForeignAddress, "se")).toBe(
      false,
    );
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
    expect(canRequestInteractiveGeocode(swedishAddress, "se")).toBe(false);
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

  it("keeps the spread evidence for an approximate address area", () => {
    const geocode = storedAddressGeocode({}, [
      {
        address_type: "visiting",
        full_address: "Campusgatan 2, 222 22 Uppsala",
        latitude: 59.802,
        longitude: 17.6025,
        geocode_status: "matched_area",
        geocode_provider: "openstreetmap",
        geocode_precision: "area",
        geocode_coordinate_supporting_point_count: 2,
        geocode_coordinate_spread_meters: 521.4,
      },
    ]);

    expect(geocode).toMatchObject({
      lat: 59.802,
      lon: 17.6025,
      precision: "area",
      coordinateSupportingPointCount: 2,
      coordinateSpreadMeters: 521.4,
    });
  });

  it("returns a stored street fallback without exact-house evidence", () => {
    const geocode = storedAddressGeocode({}, [
      {
        address_type: "visiting_or_postal",
        full_address: "DOKTOR LIBORIUS GATA 42 B, 41323 GÖTEBORG",
        latitude: 57.6815,
        longitude: 11.976175,
        geocode_status: "matched_street",
        geocode_provider: "openstreetmap",
        geocode_precision: "street",
        geocode_match_method: "postal_code_street_address_point_median",
        geocode_coordinate_supporting_point_count: 6,
        geocode_coordinate_spread_meters: 88.4,
      },
    ]);

    expect(geocode).toMatchObject({
      lat: 57.6815,
      lon: 11.976175,
      precision: "street",
      matchMethod: "postal_code_street_address_point_median",
      coordinateSupportingPointCount: 6,
    });
    expect(geocode?.sourceRecordUrl).toBeUndefined();
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
    ).toBe("City, street, and house number matched one OpenStreetMap record.");
  });

  it("explains a Sweden-wide unique street-address fallback", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "visiting_or_postal",
        full_address: "Abrahamsbergsvägen 27, 16830 BROMMA",
        geocode_status: "matched_exact",
        geocode_match_method: "country_street_house_exact_unique",
      })?.description,
    ).toBe(
      "Street and house number matched one OpenStreetMap record across Sweden.",
    );
  });

  it("labels a compact candidate cluster as an approximate site", () => {
    expect(
      addressGeocodeOutcomeCopy({
        address_type: "visiting",
        full_address: "Sitegatan 1, 111 11 Stockholm",
        geocode_status: "matched_site",
        geocode_candidate_count: 2,
        geocode_coordinate_supporting_point_count: 2,
        geocode_coordinate_spread_meters: 63.2,
      }),
    ).toEqual({
      title: "Approximate address site found",
      description:
        "2 matching OpenStreetMap records form a compact address site across roughly 63 metres. The marker is their median location, not a verified building entrance.",
      badge: "Approximate site",
    });
  });

  it("labels a wider candidate cluster as an approximate area", () => {
    const copy = addressGeocodeOutcomeCopy({
      address_type: "visiting",
      full_address: "Campusgatan 2, 222 22 Uppsala",
      geocode_status: "matched_area",
      geocode_candidate_count: 2,
      geocode_coordinate_supporting_point_count: 2,
      geocode_coordinate_spread_meters: 521.4,
    });

    expect(copy?.title).toBe("Approximate address area found");
    expect(copy?.description).toContain("address area");
    expect(copy?.description).toContain("roughly 521 metres");
    expect(copy?.description).toContain("not a verified building entrance");
  });

  it("labels a missing house as a street approximation, never a building", () => {
    const copy = addressGeocodeOutcomeCopy({
      address_type: "visiting_or_postal",
      full_address: "DOKTOR LIBORIUS GATA 42 B, 41323 GÖTEBORG",
      geocode_status: "matched_street",
      geocode_precision: "street",
      geocode_coordinate_supporting_point_count: 6,
      geocode_coordinate_spread_meters: 88.4,
    });

    expect(copy).toEqual({
      title: "Approximate street location found",
      description:
        "6 OpenStreetMap address points place this street in the supplied postal area spanning roughly 88 metres. The requested house number was not found, so the marker represents the street area, not the building.",
      badge: "Approximate street",
    });
    expect(copy?.description).not.toContain("exact building");
  });

  it("identifies a road-geometry fallback as approximate road evidence", () => {
    const copy = addressGeocodeOutcomeCopy({
      address_type: "visiting_or_postal",
      full_address: "Borgaregatan 19 B, 61131 Nyköping",
      geocode_status: "matched_street",
      geocode_precision: "street",
      geocode_match_method: "nearby_postcode_street_road_segment_median",
      geocode_coordinate_supporting_point_count: 2,
      geocode_coordinate_spread_meters: 28.1,
    });

    expect(copy).toEqual({
      title: "Approximate street location found",
      description:
        "2 OpenStreetMap road segments place this street near the supplied postal area spanning roughly 28 metres. The requested house number was not found, so the marker represents the street area, not the building.",
      badge: "Approximate street",
    });
  });

  it("flags a locality street fallback when the postcode conflicts", () => {
    const copy = addressGeocodeOutcomeCopy({
      address_type: "visiting_or_postal",
      full_address: "Furutunet 15, 18148 Lidingö",
      geocode_status: "matched_street",
      geocode_precision: "street",
      geocode_match_method:
        "street_requested_house_missing_postcode_conflict",
      geocode_coordinate_supporting_point_count: 7,
      geocode_coordinate_spread_meters: 90,
    });

    expect(copy).toEqual({
      title: "Approximate street location found",
      description:
        "7 OpenStreetMap address points place this street in the same locality, but not in the supplied postal area spanning roughly 90 metres. Neither the requested house number nor postal code was verified, so the marker represents the street area, not the building.",
      badge: "Approximate street",
    });
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
