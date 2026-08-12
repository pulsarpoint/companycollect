import { describe, expect, it } from "vitest";
import {
  foreignAddressBadgeText,
  geocodeCountryCodeForAddress,
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
