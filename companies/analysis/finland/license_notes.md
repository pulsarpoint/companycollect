# Finland — License & Terms Notes

## License: Creative Commons Attribution 4.0 International (CC-BY-4.0)

Confirmed via the avoindata.suomi.fi CKAN action API for both relevant datasets:

- Dataset `yritykset` → `license_id: cc-by-4.0`, `license_title: "Creative Commons Attribution 4.0"`
- Dataset `prh-avoin-data` → `license_id: cc-by-4.0`

Retrieved 2026-06-06 via:
`https://avoindata.suomi.fi/data/api/3/action/package_show?id=yritykset`

## What CC-BY-4.0 allows

- Copy, redistribute, and reuse the data, including commercially.
- Transform, normalize, and build derived products.

## Obligations

- **Attribution required.** Credit the source. Suggested attribution string:
  > "Contains data from the Finnish Patent and Registration Office (PRH) /
  > Business Information System (YTJ), licensed under CC-BY 4.0."
- Indicate if changes were made (normalization/derivation counts).
- Do not imply PRH endorses your product.

## Practical notes / uncertainty

- The CC-BY-4.0 designation comes from the official national open-data catalog
  metadata; treat it as authoritative but re-verify before a large redistribution.
- The open data **excludes** sole traders, personal contact data (email/phone),
  municipalities, wellbeing services counties, and tax partnerships — so there are no
  obvious personal-data redistribution concerns for the published fields, but addresses
  of small entities can still be personal data under GDPR. Apply normal GDPR care if
  publishing addresses of natural-person-operated entities.
- No paywall, no CAPTCHA, no authentication encountered. Nothing was bypassed.
- Rate limits are not published; crawl politely (throttle between pages) to stay within
  acceptable-use norms.
