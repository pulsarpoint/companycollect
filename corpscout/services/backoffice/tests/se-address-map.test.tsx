import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { mapPoint } from "~/components/admin/se-address-workspace";
import { AddressMap, type AddressMapPoint } from "~/components/detail/address-map";
import type { SeAddressRow } from "~/lib/se-company-address-entity.server";

const KEY = "c3".repeat(32);

/** A published address with a rooftop geocode; each test bends the columns it
 * is about and leaves the other 25 alone. */
function row(overrides: Partial<SeAddressRow> = {}): SeAddressRow {
  return {
    company_id: "5560125220",
    address_key: KEY,
    care_of: "",
    box: "",
    street_name: "Storgatan",
    house_number: "5",
    unit: "",
    postal_code: "11122",
    city: "Stockholm",
    country_code: "SE",
    normalized_address: "storgatan 5|11122|stockholm|se",
    kinds: ["postal"],
    sources: ["bolagsverket"],
    slots: [""],
    normalized_ids: ["norm-1"],
    text_source: "bolagsverket",
    active: 1,
    inactive_reason: "",
    latitude: 59.33,
    longitude: 18.06,
    geocode_status: "matched_exact",
    geocode_method: "osm",
    geocode_confidence: 0.98,
    geocode_precision: "building",
    geocode_policy: "v7",
    geocode_reference: "ref-1",
    geocoded_at: "2026-09-01 12:00:00.000",
    normalizer_version: "se-address-normalizer-v2",
    folded_at: "2026-09-02 08:00:00.000",
    fold_version: "fold-v1",
    source_run_id: "run-f",
    ...overrides,
  };
}

const POINT: AddressMapPoint = {
  key: KEY,
  lat: 59.33,
  lon: 18.06,
  label: "storgatan 5|11122|stockholm|se · Exact",
  approximate: false,
};

describe("mapPoint", () => {
  it("has nothing to map when either coordinate is missing", () => {
    expect(mapPoint(row({ latitude: null }))).toBeNull();
    expect(mapPoint(row({ longitude: null }))).toBeNull();
  });

  it("has nothing to map for an ungeocoded or a foreign address", () => {
    // A row no geocode run has touched, and one the Swedish reference set is
    // never going to place -- both carry coordinates in the fixture, so it is
    // the status that decides.
    expect(mapPoint(row({ geocode_status: "" }))).toBeNull();
    expect(mapPoint(row({ geocode_status: "foreign" }))).toBeNull();
  });

  it("reads a building-precision exact match as exact", () => {
    expect(mapPoint(row())?.approximate).toBe(false);
  });

  it("reads a coarse precision or a coarse status as approximate", () => {
    for (const precision of ["postcode", "city"]) {
      expect(mapPoint(row({ geocode_precision: precision }))?.approximate).toBe(true);
    }
    for (const status of ["matched_area", "matched_street"]) {
      expect(
        mapPoint(row({ geocode_status: status, geocode_precision: "" }))?.approximate,
      ).toBe(true);
    }
  });

  it("labels the point with the published line and the status label", () => {
    const point = mapPoint(row({ geocode_status: "matched_area" }));
    expect(point?.key).toBe(KEY);
    expect(point?.lat).toBe(59.33);
    expect(point?.lon).toBe(18.06);
    expect(point?.label).toBe("storgatan 5|11122|stockholm|se · Area (centroid)");
  });
});

describe("AddressMap", () => {
  it("renders its placeholder and no leaflet markup without a browser", () => {
    // The mounted flag only flips in an effect, which server rendering never
    // runs -- so react-leaflet is never imported here, which is the point of
    // the wrapper.
    const html = renderToStaticMarkup(
      <AddressMap points={[POINT]} selectedKey={KEY} onSelect={() => {}} />,
    );
    expect(html).toContain("bg-muted");
    expect(html).not.toContain("leaflet");
    expect(html).not.toContain("No location");
  });

  it("says there is no location when no point can be mapped", () => {
    const html = renderToStaticMarkup(
      <AddressMap points={[]} selectedKey={null} onSelect={() => {}} />,
    );
    expect(html).toContain("No location");
  });
});
