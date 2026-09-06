# Bolagsverket API portal — what we have, what we lack, how to get it

Status: verified 2026-09-04 against the live developer portal
(`portal.api.bolagsverket.se`, WSO2 API Manager) and the gateway, using our
existing production credentials in a read-only probe (token + `/isalive`
per API; no data written).

## The short answer

**We already have a Bolagsverket API portal account and application** —
the OAuth2 client used by `sweden_bolagsverket_vdm`
(`BOLAGSVERKET_VDM_CLIENT_ID/SECRET`, token endpoint
`portal.api.bolagsverket.se/oauth2/token`; onboarding note in
`docs/sweden-api/data.txt`). That application is **subscribed to exactly one
API**: *VärdefullaDatamängder*. The beneficial-ownership, mortgage and
notification data live in **other APIs on the same portal**, which our
application is **not subscribed to** — the gateway answers
`403 900908 "API Subscription validation failed"` for all of them.

So: yes, a request is needed, but it is an *additional subscription* on an
account we already hold, not a new onboarding.

## The APIs on the portal (7 total)

| API | Context | What it gives | Our status |
|---|---|---|---|
| **VärdefullaDatamängder** v1 | `/vardefulla-datamangder/v1` | `POST /organisationer` (company), `POST /dokumentlista`, `GET /dokument/{id}` (annual reports) | **subscribed, in production** |
| **VerkligaHuvudmän** v1 | `/verkliga-huvudman/v1` | `POST /organisationer` — beneficial owners of an org; `POST /personer` — every org where a person is beneficial owner | not subscribed (403 900908) |
| **Företagsinformation** v4 | `/foretagsinformation/v4` | `POST /organisationer`, `/personer`, `/firmateckningsalternativ`, `/arenden` (cases), `/aktiekapitalforandringar` (share-capital changes), `/organisationsengagemang` (a person's/company's roles elsewhere), `/finansiella-rapporter`, **`/foretagsinteckningar` (business mortgages)** | not subscribed |
| **Notifiering** v1 | `/notifiering/v1` | subscription-based **change notifications**: configure a *prenumeration*, attach `dataidentiteter` (org numbers), receive notifications, request re-sends — the events feed | not subscribed |
| **Dokument** v1 | `/dokument/v1` | public documents per organisation | not subscribed |
| NordicInformation v1 | `/nordic-information/v1` | Nordic company info (Nordic Smart Government) | not subscribed |
| SSBTGO v1 | `/ssbtgo/v1` | SSBT (internal/partner) | not relevant |

Key finding: **business mortgages are an operation of Företagsinformation
v4**, not of VärdefullaDatamängder; **beneficial owners are their own API**;
**the Bolagsverket events feed we wanted exists as Notifiering**.

## Which are free

- **VärdefullaDatamängder** — the EU high-value-dataset API: free, no
  agreement (we have it).
- **VerkligaHuvudmän** and **Företagsinformation v4** — `monetization:
  enabled=false` on the portal, but both are published with **throttling
  tiers `500 / 3000 / 8000 / 15000`** (requests per period; tier chosen at
  subscription). Bolagsverket's own statement is that the *high-value*
  datasets are free for everyone; these two APIs carry data outside the
  high-value set (beneficial ownership, mortgages, cases, share capital),
  which Bolagsverket has historically sold under agreement. The portal
  metadata does not state a price; **expect an agreement and possibly a fee,
  tier-dependent** — to be confirmed with `api@bolagsverket.se`.
- **Notifiering** — no tiers listed; terms to confirm (technical owner
  `ssbt@bolagsverket.se`). Likely bundled with a Företagsinformation
  subscription since notifications concern that data.
- **Dokument** — documents are public; likely free; confirm.

Beneficial-owner responses are explicitly **scope-dependent**: *"Full or
limited response depending on OAuth 2.0 scope."* The subscription decides
whether we get full owner identities or a limited view — ask for the scope
explicitly.

## How to request

Two routes, use both:

1. **Self-service on the developer portal** — log in to
   `https://portal.api.bolagsverket.se/devportal/`, open the *application*
   that holds our existing keys, and **subscribe** it to
   *VerkligaHuvudmän*, *Företagsinformation v4* and *Notifiering*
   (choose a tier). WSO2 subscriptions to restricted APIs typically go to an
   approval workflow — the gateway will keep returning 900908 until
   Bolagsverket approves.
2. **Email `api@bolagsverket.se`** (the business owner on all three APIs)
   in parallel, referencing our existing application and asking for:
   the subscriptions, the intended tier, the **full-response scope** for
   VerkligaHuvudmän, the terms/fee, and whether Notifiering requires a
   separate agreement.

Draft (Swedish):

> Ämne: Prenumeration på ytterligare API:er för befintlig applikation
>
> Hej,
>
> Vi har sedan tidigare en applikation i er API-portal med prenumeration på
> API:et VärdefullaDatamängder (client id <id>, organisation <bolag>,
> org.nr <nr>).
>
> Vi önskar utöka applikationen med prenumeration på:
> - VerkligaHuvudmän v1 — med scope för fullständigt svar
> - Företagsinformation v4 — särskilt operationerna /foretagsinteckningar,
>   /arenden, /aktiekapitalforandringar och /organisationsengagemang
> - Notifiering v1 — för förändringsnotifieringar på de organisationer vi
>   följer
>
> Avsett ändamål: uppdatering av vårt företagsregister med aktuella
> uppgifter om verkliga huvudmän, företagsinteckningar och
> registerhändelser, med regelbunden hämtning för de organisationer vi
> följer.
>
> Vänligen återkom med villkor, eventuell avgift per nivå (500/3000/8000/
> 15000), samt om Notifiering kräver separat avtal.
>
> Med vänlig hälsning, <namn, titel, telefon>

## Notes for the eventual build

- Same OAuth2 client-credentials flow and gateway host as the existing
  `BolagsverketVdmResource`; the new APIs can reuse its token handling,
  request-ID and no-PII-logging discipline. All lookups are `POST` with the
  identity in the body (Bolagsverket deliberately avoids org/person numbers
  in URLs) — the existing `_post_json` shape fits.
- Beneficial-owner and `/personer` data is personal data; store hashed
  identities in object keys as the VDM module already does, and keep the
  serving-layer PII handling of the people tables.
- Notifiering turns the weekly sweep into an event stream: register the
  active-priority tier's org numbers as `dataidentiteter` in a subscription
  and consume notifications, with `omsandningar` for replay after outages.
- `/organisationsengagemang` (a company's roles in other companies) plus
  VerkligaHuvudmän give the ownership graph needed for group-level dedup
  and rollups — the SCB koncern data becomes unnecessary.
