# Japan — License Notes

## NTA Corporate Number Publication (法人番号公表サイト) — free use

- The National Tax Agency publishes the **basic three items** of every
  corporation's number information (法人番号 / corporate number, 商号又は名称 /
  name, 本店所在地 / address). The NTA explicitly states this published
  information may be **used freely by anyone**, including reproduction,
  processing, and redistribution, for any purpose including commercial use — **no
  application is required** for the published/downloaded data.
- (An application/ID is required only to call the **Web-API**, and a separate
  registration exists for adding an English name — neither affects reuse of the
  downloaded data.)
- Treatment here: **open / free use**. Attribution to the NTA is good practice
  though not mandated. Corporate identity data (number, name, address) is public
  by statute and is **not personal data**.

## EDINET (FSA) — public disclosure

- EDINET hosts statutory disclosure documents (securities reports etc.). The
  content is public disclosure. The **API v2 requires a free Subscription-Key**
  (registration with the FSA); v1 is retired. Respect the EDINET terms of use and
  rate limits. Redistribution of bulk XBRL should follow the EDINET terms.
- Treatment here: **blocked_by_authentication** (free key); data is public
  disclosure. Catalog only — no records fetched without a key.

## gBizINFO (METI) — government standard terms of use

- gBizINFO data is provided under the Japanese **government standard terms of use
  (政府標準利用規約)**, which is compatible with **CC-BY 4.0** (free use with
  attribution). The REST API and bulk CSV require a **free token** (利用申請).
- Treatment here: **blocked_by_authentication** (free token); reusable with
  attribution once obtained. Catalog only.

## Legal Affairs Bureau registry (登記情報提供サービス) — paid / restricted

- The full commercial register (officers, capital, purpose, history) is provided
  on a **pay-per-record** basis by the Ministry of Justice. No open bulk/API.
- Treatment here: **blocked_by_payment**. Cataloged from public documentation
  only; no raw values copied.

## Personal data note

- The NTA identity data concerns **corporations**, not individuals, so it is not
  personal data. However, **officer/director/representative** information (from
  the paid registry or aggregators) is personal data under Japan's **APPI** (Act
  on the Protection of Personal Information). Any such data must be handled per
  APPI and redacted in committed samples. No officer data is included here.
