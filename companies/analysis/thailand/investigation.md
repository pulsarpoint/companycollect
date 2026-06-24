# Thailand — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Thailand, download/sample where allowed, and document a
reproducible trail.

## What was found

### 1. DBD OpenAPI — open juristic-person API (RECOMMENDED)

- **`https://openapi.dbd.go.th/api/v1/juristic_person/{13-digit-id}`** — Department
  of Business Development (DBD), Ministry of Commerce. **Fully open (no API key)** —
  verified live with real companies:
  - **0107544000108 → PTT PUBLIC COMPANY LIMITED** (register capital ฿28,562,996,250)
  - **0107536000374 → BANGKOK BANK PUBLIC COMPANY LIMITED** (฿40,000,000,000)
  - **0107542000011 → CP ALL PUBLIC COMPANY LIMITED** (฿8,986,296,048)
  - **0107544000094 → INTERNET THAILAND PUBLIC COMPANY LIMITED** (฿596,740,267)
- Response is JSON (`status.code = "1000" Success`) with
  `cd:OrganizationJuristicPerson` fields:
  - `cd:OrganizationJuristicID` (13-digit), `…NameTH`, `…NameEN`, `…Type`
    (บริษัทมหาชนจำกัด etc.), `…RegisterDate` (YYYYMMDD), `…Status`
    (ยังดำเนินกิจการอยู่ = active),
  - `cd:OrganizationJuristicObjective` → `td:JuristicObjective` with TSIC
    `td:JuristicObjectiveCode` + TH/EN text,
  - `cd:OrganizationJuristicRegisterCapital`, `…PaidUpCapital` (THB),
  - `cd:OrganizationJuristicBranchName`,
  - `cd:OrganizationJuristicAddress` → fully structured (Address, Building, AddressNo,
    Road, CitySubDivision/City/CountrySubDivision with codes).
- **Per-company lookup** by the 13-digit ID. No open bulk enumeration endpoint was
  found, but each company's record is fully open. **The strongest open official
  company API found in SE Asia in this project so far.**

### 2. DBD DataWarehouse — full financial statements (login-gated)

- **`datawarehouse.dbd.go.th`** — DBD's company-profile + **financial-statement**
  warehouse (balance sheet / income statement, filed annually by all juristic
  persons). It returned **HTTP 302/403** for automated/API requests
  (login/session-gated). The richest **financial** source, but not open for
  automation.

### 3. data.go.th — national open-data portal (WAF-blocked here)

- **`data.go.th`** (CKAN) hosts DBD juristic-person datasets, but the portal and its
  CKAN API returned **HTTP 403 "Access Denied"** (WAF) for automated requests from
  this environment. Not usable headless here.

### 4. SET — Stock Exchange of Thailand (listed financials)

- **`set.or.th`** publishes **listed-company** financial statements and disclosures
  (302 redirect; public via browser). Listed companies only.

### 5. Tax / VAT

- The Revenue Department (`rd.go.th`) handles VAT (ภ.พ.20). Thailand uses **one
  13-digit number** as both the **juristic registration number and the Tax ID**;
  VAT registration uses the **same** 13-digit ID (no separate VAT number).

## Conclusion

Thailand has an **excellent open official company API** — the **DBD OpenAPI** —
returning real identity, type, status, register date, **TSIC activity**, **register
& paid-up capital (THB)**, and **structured address** per **13-digit juristic ID**,
with **no token**. Full **financial statements** are in the **DBD DataWarehouse**
(login-gated) and **SET** (listed). The 13-digit juristic ID **is also the Tax ID**;
there is no separate VAT number. Recommended ingestion is **per-company DBD OpenAPI
lookup** + DataWarehouse/SET for financials. Currency **THB**; data in Thai +
English. Directors/shareholders are personal data (PDPA) and are not in the open API.
