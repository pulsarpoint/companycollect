/** EU-27 plus the three EEA EFTA states, for the procurement country filter.
 * EEA is included because TED carries Norwegian notices; a strict EU list
 * would grey out a country that has data. Sorted by English name.
 *
 * `iso3` is included alongside `iso2` because register `country_code`
 * columns and TED buyer/winner country fields are not consistently one or
 * the other (e.g. a winner row carries "CZE" while the register stores
 * "cz") — see `~/lib/company-match.ts`, which needs both forms to resolve a
 * row's country. */
export const EU_EEA_COUNTRIES: { iso2: string; iso3: string; name: string }[] = [
  { iso2: "AT", iso3: "AUT", name: "Austria" },
  { iso2: "BE", iso3: "BEL", name: "Belgium" },
  { iso2: "BG", iso3: "BGR", name: "Bulgaria" },
  { iso2: "HR", iso3: "HRV", name: "Croatia" },
  { iso2: "CY", iso3: "CYP", name: "Cyprus" },
  { iso2: "CZ", iso3: "CZE", name: "Czechia" },
  { iso2: "DK", iso3: "DNK", name: "Denmark" },
  { iso2: "EE", iso3: "EST", name: "Estonia" },
  { iso2: "FI", iso3: "FIN", name: "Finland" },
  { iso2: "FR", iso3: "FRA", name: "France" },
  { iso2: "DE", iso3: "DEU", name: "Germany" },
  { iso2: "GR", iso3: "GRC", name: "Greece" },
  { iso2: "HU", iso3: "HUN", name: "Hungary" },
  { iso2: "IS", iso3: "ISL", name: "Iceland" },
  { iso2: "IE", iso3: "IRL", name: "Ireland" },
  { iso2: "IT", iso3: "ITA", name: "Italy" },
  { iso2: "LV", iso3: "LVA", name: "Latvia" },
  { iso2: "LI", iso3: "LIE", name: "Liechtenstein" },
  { iso2: "LT", iso3: "LTU", name: "Lithuania" },
  { iso2: "LU", iso3: "LUX", name: "Luxembourg" },
  { iso2: "MT", iso3: "MLT", name: "Malta" },
  { iso2: "NL", iso3: "NLD", name: "Netherlands" },
  { iso2: "NO", iso3: "NOR", name: "Norway" },
  { iso2: "PL", iso3: "POL", name: "Poland" },
  { iso2: "PT", iso3: "PRT", name: "Portugal" },
  { iso2: "RO", iso3: "ROU", name: "Romania" },
  { iso2: "SK", iso3: "SVK", name: "Slovakia" },
  { iso2: "SI", iso3: "SVN", name: "Slovenia" },
  { iso2: "ES", iso3: "ESP", name: "Spain" },
  { iso2: "SE", iso3: "SWE", name: "Sweden" },
];
