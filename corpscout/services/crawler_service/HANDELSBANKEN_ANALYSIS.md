# Handelsbanken company analysis

Research date: 16 September 2026. **Completed with documented coverage and validation limitations.**

This analysis covers the Swedish bank website, relevant official group pages and employer-linked job advertisements. It separates an autonomous crawler run from auditor-selected follow-up sources. It does not claim exhaustive coverage of every branch, product page or country. Financial and certificate documents are discovered as links; their contents have not been parsed.

Model stages used **DeepSeek V4.1 Flash**, alias `deepseek-flash`, directly through `https://api.deepseek.com`, with **low reasoning effort**. Codex performed the source audit, guided follow-ups and code fixes. The separate low/high experiment requested afterward is recorded in `data/handelsbanken-20260916-reasoning/`; it does not overwrite this original analysis. [DeepSeek model documentation](https://api-docs.deepseek.com/quick_start/pricing/).

## Company and business

**Svenska Handelsbanken AB (publ)** is a Swedish banking company founded in 1871. Its home markets are Sweden, Norway, the United Kingdom and the Netherlands, with additional presence in Luxembourg and the United States. The website describes a decentralised bank with local decision-making and 407 branches/meeting places; that last figure has no explicit measurement date on the page. [Group overview](https://www.handelsbanken.com/sv/om-koncernen).

The Swedish website serves retail and business customers. The group website carries corporate governance, ownership, investor relations and group-level information. These are two domains describing the same banking group, so a domain-only crawl boundary loses valuable company context.

| Identifier | Value |
|---|---|
| Legal name | Svenska Handelsbanken AB (publ) |
| Swedish registration number | 502007-7862 |
| LEI | NHBDILHZTYCNBV5UYZ31 |
| SWIFT/BIC | HANDSESS |
| GIIN | TT0D1B.00000.LE.752 |
| Headquarters | Kungsträdgårdsgatan 2, SE-106 70 Stockholm, Sweden |
| Switchboard | +46 8 701 10 00 |

Source: [Group contact information](https://www.handelsbanken.com/sv/kontakta-oss).

Its service families include:

- **Personal finance:** mortgages, savings and investments, pension products, accounts, cards and payments. Premium advice and Private Banking address customers wanting broader financial advice. [Swedish homepage](https://www.handelsbanken.se/sv/).
- **Business banking:** financing, business savings, corporate cards and payments, pensions for owners/employees, and interest-rate/currency risk management. Its Hållbarhetskollen offering helps business customers estimate emissions using accounting data. [Business banking](https://www.handelsbanken.se/sv/foretag).
- **Capital markets and investment banking:** specialist services sit within Handelsbanken Markets. [Group organisation](https://www.handelsbanken.com/sv/om-koncernen/organisation).
- **Open Banking:** access to bank APIs through a developer portal and sandbox for third parties. This is an offered integration service; the term “API” is not itself a specific software product. [Open Banking](https://www.handelsbanken.com/en/open-banking).

## Ownership and connected companies

The shareholder page is explicitly dated **31 December 2025**. The leading reported voting stakes are Industrivärden **11.8%**, Oktogonen Foundation **8.2%**, Lundberg group **4.9%**, BlackRock **3.6%** and Vanguard **3.3%**. These are voting percentages, not automatically equity percentages or current controlling ownership. The structured audit retains all 15 listed shareholders and the observation date. [Shareholders](https://www.handelsbanken.com/sv/investor-relations/aktien/vara-aktieagarna).

The official group page explicitly describes these five companies as wholly owned:

| Subsidiary | Activity |
|---|---|
| Handelsbanken Fonder AB | Fund management |
| Handelsbanken Liv Försäkringsaktiebolag | Life insurance |
| Handelsbanken plc | UK banking |
| Stadshypotek AB | Mortgage finance |
| EFN Ekonomikanalen AB | Economic and financial news |

This is a published selection of subsidiaries, not a complete legal-entity register. [Subsidiaries](https://www.handelsbanken.com/en/about-the-group/organisation/subsidiaries).

EFN is a particularly useful external-link example: the Swedish homepage explicitly states that Handelsbanken owns it, and the subsidiaries page corroborates that relationship. An external header/footer link alone would not establish ownership. Links to government articles, social networks, an IR document host or a recruitment platform require different relationship descriptions.

## People and business contacts

The official management page lists eight executives:

| Person | Role |
|---|---|
| Michael Green | CEO and President |
| Mårten Bjurman | CFO |
| Cecilia Lundin | Head of HR |
| Henrik Agebäck | Acting Head of IT |
| Pernilla Eldestrand | Head of Communications |
| David Haqvinsson | Head of Credit |
| Maria Hedin | Head of Independent Risk Control |
| Dan Lindwall | Responsible for subsidiaries and Executive Vice President |

Source: [Executive management](https://www.handelsbanken.com/sv/om-koncernen/organisation/verkstallande-ledning). Preserve “acting” for the IT role.

Pär Boman is board chair and Fredrik Lundberg vice chair. The board page identifies eight shareholder-elected members following the 25 March 2026 AGM, with employee representatives in addition. [Board](https://www.handelsbanken.com/sv/om-koncernen/organisation/styrelse).

Published business contacts include Peter Grabe (Investor Relations), Andreas Skogelid (debt investors/rating), Lars Kenneth Dahlqvist and Susanna Överby (IR). The press office is `press@handelsbanken.se`, **+46 8 701 80 18**. Their individual business email addresses and telephone numbers are retained in the structured audit. [Contacts](https://www.handelsbanken.com/sv/kontakta-oss).

The sustainability team publishes `sustainability@handelsbanken.se`. [Sustainability governance](https://www.handelsbanken.com/en/sustainability/sustainability-governance).

## Jobs and technology evidence

The two captured Swedish careers pages contain **32 entries, including 16 categorised as IT**. All 32 linked job pages were fetched successfully, and all contain employer-supplied `JobPosting` JSON-LD. Two entries are speculative applications, leaving 30 advertisements for defined roles. Clicking the observed second-page button recovered the last two IT entries. This is the Swedish listing at capture time, not a complete global vacancy count. [Swedish vacancies](https://www.handelsbanken.se/sv/om-oss/jobba-hos-oss/lediga-jobb).

The careers page describes more than 2,000 people working in tech, data and innovation, with most IT infrastructure developed and maintained in-house. Stockholm and Malmö are named IT locations. This is an undated employer website claim. [Technology careers](https://www.handelsbanken.se/sv/om-oss/jobba-hos-oss/tech-data-innovation).

Strong, specific technology signals from individual ads include:

| Technology | What the source supports |
|---|---|
| [Microsoft 365, Exchange](https://emp.jobylon.com/jobs/365650-svenska-handelsbanken-ab-microsoft-365-specialist-med-exchange-bakgrund/) | Explicitly existing environment operated and maintained by the bank’s team. |
| [SAS](https://emp.jobylon.com/jobs/382400-svenska-handelsbanken-ab-sas-utvecklare-inom-financial-crime-prevention/) | Development and maintenance of financial-crime-prevention systems in the SAS platform. |
| [Salesforce](https://emp.jobylon.com/jobs/370539-svenska-handelsbanken-ab-tech-lead-salesforce-inom-kundplattform-crm-till-handelsbanken/) | CRM development and formation of a new Salesforce team. |
| [Java, Jakarta EE, Git](https://emp.jobylon.com/jobs/378195-svenska-handelsbanken-ab-javautvecklare-till-handelsbanken-stockholm-malmo/) | Named application-development stack, with variation between teams. |
| [Azure Managed HSM, Azure Key Vault](https://emp.jobylon.com/jobs/353588-svenska-handelsbanken-ab-azure-infrastructure-engineer-key-management-till-handelsbanken/) | Operation and maintenance of security-critical key-management infrastructure. |
| [JavaScript, TypeScript, React](https://emp.jobylon.com/jobs/370651-svenska-handelsbanken-ab-fullstackutvecklare-till-handelsbanken-forma-framtidens-internationella-digitala-kundmote/) | Frontend development in the international digital customer-experience role. |
| [Microsoft Fabric, Azure Databricks](https://emp.jobylon.com/jobs/370782-svenska-handelsbanken-ab-losningsarkitekt-inom-data-platforms-med-fokus-pa-analys-och-rapportering-pa-handelsbanken/) | Implementation of a target data-platform architecture; preserve the development/modernisation context. |

Each observation stays attached to its job, employer, source URL and team/role scope. The original workflow plus a targeted validation-fix replay retained **34 of 38 source-read technology identity controls**. Strict signal/scope matching passes 32; the Fabric/Databricks target-architecture context requires qualified interpretation. These selected controls measure retention, not overall accuracy or precision.

The pipeline accepted 103 observations across 64 raw technology labels: 16 stated-use, 52 required-experience and 35 preferred-experience observations. Five observations have additional audit holds, leaving 98 grouped into 58 identities after explicit aliases. These are not 58 deployed products. The audit holds WCAG as a standard, CSS outside the specific-product view, an insufficiently qualified Private Endpoints identity, and the combined `Java/Jakarta EE` identity. Separate source-audit supplements preserve the latter's two technologies, Jira and Azure Pipelines without pretending they passed the automated workflow. Eight descriptions and 15 statement IDs from the first 14 ads remain unresolved.

Important weaker signals include dbt, Prefect, Terraform and Ansible as desirable candidate experience; DB2 **or** SQL Server as alternatives; and Jira/Zephyr/Confluence as plus-point experience. These should remain searchable hiring signals, without turning them into confirmed deployment claims. HTML, JSON, XML, generic AI and CI/CD are excluded under this project's technology rules.

Public contact extraction from the job descriptions saved **120 email occurrences, covering 70 distinct `handelsbanken.se` addresses**. These are occurrences with nearby source context, not 70 independently resolved people or hiring managers. The employer-supplied JSON-LD is preserved separately for comparison.

## Certifications, compliance and documents

No bank-wide ISO 27001, ISO 14001 or PCI DSS certification was established from the reviewed HTML. That means **not established**, not “the bank has no certifications.” Candidate Salesforce or security qualifications must not be promoted to company-held credentials. Awards, credit ratings and a legal-document index also do not establish ISO/PCI certification.

The sustainability page states that the 2025 sustainability report follows ESRS and includes EU Taxonomy reporting. This is a reporting/compliance statement with a defined period, not a certificate. [Sustainability governance](https://www.handelsbanken.com/en/sustainability/sustainability-governance).

The group report archive embeds an external HTML page. Following that iframe recovered **58 document-link occurrences** on its first archive page, including annual/interim reports, presentations, factbooks and regulatory disclosures. They have not been parsed. [Group archive](https://www.handelsbanken.com/sv/investor-relations/rapporter-och-presentationer), [embedded archive](https://vp292.alertir.com/sv/node/4).

Useful documents include:

- [Annual report 2025 — PDF](https://vp292.alertir.com/afw/files/press/handelsbanken/202602267076-1.pdf).
- [Annual report 2025 — XHTML](https://vp292.alertir.com/sites/default/files/report/ars-_och_hallbarhetsredovisning_2025.xhtml).
- [January–June 2026 interim report](https://vp292.alertir.com/afw/files/press/handelsbanken/202607148882-1.pdf).
- [Handelsbanken plc annual report 2025](https://vp292.alertir.com/sites/default/files/report/hb_26_0039_-_2026_uk_annual_report_150426.pdf).
- [Handelsbanken Liv solvency and financial condition report 2025](https://vp292.alertir.com/sites/default/files/report/rapport_om_solvens_och_finansiell_stallning_2025.pdf).

## External links

The combined saved pages contain **507 external-link occurrences, 100 distinct full URLs and 27 destination domains**. All occurrences retain their source page, HTML provenance, link text, region and surrounding context. DeepSeek assessed all 507: 457 assessments are contextual hints and 50 cite explicit text. Passing schema/evidence checks is not a 100% relationship-accuracy claim.

The source operator matters. **128 MapTiler/OpenStreetMap occurrences are on Jobylon-hosted advertisements** and describe the job platform's map stack; they are not evidence of Handelsbanken's internal stack. Footer country links suggest group affiliations but do not establish a legal ownership chain. Six inferred customer relationships from job-platform links remain unverified. By contrast, the developer-support page expressly lists collaboration with Bankgirot, Swish and Fortnox; that supports a collaboration claim, not shareholding. [Developer support](https://www.handelsbanken.se/sv/foretag/konton-betalningar/utvecklarstod).

## Crawler evaluation

The autonomous diagnostic fetched 12 pages and made 112 model calls before I stopped it for guided completion. It had not reached the jobs list or group ownership pages. It is saved as **partial**, with the original pre-stop snapshot and interruption metadata. This was an analyst stop, not a completed 30-page budget run. The finished company analysis therefore combines that diagnostic with explicitly identified follow-ups.

Two implementation defects were fixed:

1. A numeric `tel:` link was mistaken for an HTTP URL and broke the pilot's first page processing. Version 0.15.1 rejects non-HTTP schemes explicitly.
2. Converting company/team/role observations to job-role scope could create duplicate observations and reject an otherwise correct technology. Version 0.15.2 coalesces duplicates created by conversion, while continuing to reject genuine duplicate model output. Replaying the saved Microsoft 365/Exchange decisions recovered four observations and improved the controls from 32/38 to 34/38.

The remaining priorities are earlier coverage of objective-specific hubs across confirmed company domains, generic pagination and iframe discovery, carrying page/operator context into contact validation, and splitting compound technology identities. Stronger reasoning alone cannot expose a pagination page or embedded archive that the fetch/discovery layer never supplied.

The sitemap was found through robots.txt after `/sitemap.xml` returned 404. It contains 1,018 URLs; the autonomous inventory cap admitted 1,000. The English vacancies page returned no entries while the Swedish page exposed 32 across two pages, so language variants need independent coverage checks.

Across the original pilot, autonomous diagnostic, job workflows, scope recovery and external-link review, **280 requests** were made. Usage returned for 278 implies **approximately $1.151** at the documented off-peak tariff; two interrupted requests have unknown usage/cost. This is a token-based estimate, not a provider bill, and excludes the later reasoning comparison. Per-run accounting is in `model-usage.json`.

Validation: 144 regular tests and four real-browser integration tests passed, including direct DeepSeek request formatting and duplicate-conversion behavior. Ruff and source type checks passed. No backend data was submitted or deployed.

## Saved artifacts

Paths are relative to this report's directory:

- `data/handelsbanken-20260916-review/company-analysis.json`: combined reviewed output, including provenance, coverage gaps, holds and external links.
- `data/handelsbanken-20260916-full/crawl/result.json`: partial autonomous diagnostic, explicitly stopped for guided completion.
- `data/handelsbanken-20260916-guided/sources/`: 16 separately selected audit sources, native cleaned and rendered HTML.
- `data/handelsbanken-20260916-guided/jobs/` and `jobs-page2/`: all 32 job pages and captured listing data.
- `data/handelsbanken-20260916-review/jobs.json`: consolidated source-derived job index and publisher metadata.
- `data/handelsbanken-20260916-guided/job-technologies/`: completed statement workflow on the first 14 IT ads; `job-technologies-page2/` contains the final two. Corresponding `-recovered/` folders preserve the targeted scope replay.
- `data/handelsbanken-20260916-review/reviewed-company.json`: source-audited company, people, contacts, ownership and report links.
- `data/handelsbanken-20260916-review/technology-controls.json`: 33 source-read technology identity/scope controls; `technology-controls-page2.json` adds five more.
- `data/handelsbanken-20260916-review/job-contact-occurrences.json`: source-linked public job contact occurrences.
- `data/handelsbanken-20260916-review/external-link-review/`: 507 contextual assessments and original inventory.
- `data/handelsbanken-20260916-review/model-usage.json`: token and estimated-cost accounting, including unknown interrupted usage.

No backend data was submitted or deployed.
