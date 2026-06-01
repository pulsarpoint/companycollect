# Corpscout — Country Source Index

Top 100 countries by GDP (IMF 2024 nominal). Last updated: 2026-05-18. All 100 files present. Update a country file directly when new sources are found. One file per country documenting
all known company data sources: access method, cost, fields, and adapter status.

To update: edit the country file directly and update the row below if the
"Best Free Option" or "API?" column changes.

| Rank | Country | ISO2 | Best Free Option | Free API? | Adapter |
|------|---------|------|-----------------|-----------|---------|
| 1 | United States | US | SEC EDGAR / state registries | Partial | — |
| 2 | China | CN | SAMR search | No | — |
| 3 | Germany | DE | Handelsregister (offeneregister.de bulk) | Bulk DL | — |
| 4 | Japan | JP | National Tax Agency (CSV) | Bulk DL | — |
| 5 | India | IN | MCA21 (Ministry of Corporate Affairs) | Partial | — |
| 6 | United Kingdom | GB | Companies House | Yes | `companies_house` |
| 7 | France | FR | Infogreffe / INSEE SIRENE | Yes (SIRENE) | — |
| 8 | Italy | IT | Registro Imprese (scrape) | No | — |
| 9 | Canada | CA | Corporations Canada (federal) | Bulk DL | — |
| 10 | Brazil | BR | CNPJ (Receita Federal bulk) | Bulk DL | — |
| 11 | Russia | RU | EGRUL (Federal Tax Service) | Bulk DL | — |
| 12 | South Korea | KR | DART / BizInfo | Partial | — |
| 13 | Australia | AU | ASIC / ABN Lookup | Yes | — |
| 14 | Mexico | MX | SAT / Registro Público Comercio | No | — |
| 15 | Spain | ES | BORME / Registro Mercantil | Scrape | — |
| 16 | Indonesia | ID | AHU Online (Ministry of Law) | No | — |
| 17 | Netherlands | NL | KVK (Chamber of Commerce) | API (paid) | — |
| 18 | Saudi Arabia | SA | Ministry of Commerce | No | — |
| 19 | Turkey | TR | MERSİS / Trade Registry | No | — |
| 20 | Switzerland | CH | Zefix (federal) | Yes | — |
| 21 | Taiwan | TW | Ministry of Economic Affairs | Bulk DL | — |
| 22 | Poland | PL | KRS (National Court Register) | Yes | — |
| 23 | Argentina | AR | IGJ / provincial registries | No | — |
| 24 | Sweden | SE | Bolagsverket | API (paid) | — |
| 25 | Belgium | BE | CBE (Crossroads Bank) | API (free) | — |
| 26 | Thailand | TH | DBD (Department of Business Development) | No | — |
| 27 | Norway | NO | Brreg | Yes | `brreg` |
| 28 | Israel | IL | Companies Registrar | No | — |
| 29 | Austria | AT | Firmenbuch | Scrape | — |
| 30 | UAE | AE | MoCIAT / Dubai DED | No | — |
| 31 | Nigeria | NG | CAC (Corporate Affairs Commission) | No | — |
| 32 | Singapore | SG | ACRA (BizFile+) | API (paid) | — |
| 33 | Malaysia | MY | SSM (Companies Commission) | No | — |
| 34 | South Africa | ZA | CIPC | Bulk DL | — |
| 35 | Egypt | EG | GAFI | No | — |
| 36 | Bangladesh | BD | RJSC | No | — |
| 37 | Philippines | PH | SEC (scrape) | No | — |
| 38 | Denmark | DK | CVR | Yes | `cvr` |
| 39 | Pakistan | PK | SECP | No | — |
| 40 | Vietnam | VN | National Business Registration Portal | No | — |
| 41 | Colombia | CO | Confecámaras / RUE | No | — |
| 42 | Hong Kong | HK | Companies Registry | Bulk DL | — |
| 43 | Romania | RO | ONRC | Scrape | — |
| 44 | Czech Republic | CZ | OR (Obchodní rejstřík) | Yes | — |
| 45 | Chile | CL | CBR / SII | Partial | — |
| 46 | New Zealand | NZ | Companies Office | Yes | — |
| 47 | Finland | FI | PRH (Finnish Patent and Registration Office) | Yes | — |
| 48 | Portugal | PT | Racius / IRN | Scrape | — |
| 49 | Peru | PE | SUNARP | No | — |
| 50 | Kazakhstan | KZ | Business Information System | No | — |
| 51 | Greece | GR | GEMI (General Commercial Registry) | Yes | — |
| 52 | Iraq | IQ | Ministry of Trade | No | — |
| 53 | Algeria | DZ | CNRC | No | — |
| 54 | Hungary | HU | Cégbíróság (Company Court) | Yes | — |
| 55 | Qatar | QA | Ministry of Commerce | No | — |
| 56 | Morocco | MA | RC (Registre de Commerce) | No | — |
| 57 | Slovakia | SK | Obchodný register | Yes | — |
| 58 | Ethiopia | ET | Ministry of Trade | No | — |
| 59 | Kenya | KE | eCitizen / BRS | No | — |
| 60 | Kuwait | KW | Ministry of Commerce | No | — |
| 61 | Ecuador | EC | Superintendencia de Compañías | Bulk DL | — |
| 62 | Dominican Republic | DO | Registro Mercantil | No | — |
| 63 | Uzbekistan | UZ | My.gov.uz | No | — |
| 64 | Tanzania | TZ | BRELA | No | — |
| 65 | Guatemala | GT | Registro Mercantil | No | — |
| 66 | Oman | OM | Ministry of Commerce | No | — |
| 67 | Angola | AO | IGAPE | No | — |
| 68 | Bulgaria | BG | Commercial Register (BRRA) | Yes | — |
| 69 | Ghana | GH | ORC (Registrar General) | No | — |
| 70 | Lithuania | LT | Registrų centras (RC) | Yes | — |
| 71 | Côte d'Ivoire | CI | CEPICI | No | — |
| 72 | Serbia | RS | APR portal (free registration) | No* | — |
| 73 | Luxembourg | LU | LBR (Luxembourg Business Register) | API (paid) | — |
| 74 | Croatia | HR | Sudski registar | Yes | — |
| 75 | Belarus | BY | Ministry of Justice | No | — |
| 76 | Azerbaijan | AZ | Ministry of Economy | No | — |
| 77 | Myanmar | MM | DICA | No | — |
| 78 | Uganda | UG | URSB | No | — |
| 79 | Sri Lanka | LK | ROC | No | — |
| 80 | Slovenia | SI | AJPES | Yes | — |
| 81 | Tunisia | TN | RNE | No | — |
| 82 | Jordan | JO | Ministry of Industry & Trade | No | — |
| 83 | Cameroon | CM | CFCE | No | — |
| 84 | Libya | LY | — | No | — |
| 85 | Bahrain | BH | MOICT Sijilat | No | — |
| 86 | Bolivia | BO | SEPREC | No | — |
| 87 | Paraguay | PY | MIC / SET | No | — |
| 88 | Latvia | LV | Lursoft / UR | Partial | — |
| 89 | Estonia | EE | Ariregister | Yes | `ariregister` |
| 90 | Nepal | NP | OCR | No | — |
| 91 | Senegal | SN | APIX | No | — |
| 92 | Albania | AL | QKB | Yes | — |
| 93 | Mozambique | MZ | CPAR | No | — |
| 94 | Zambia | ZM | PACRA | No | — |
| 95 | DR Congo | CD | OHADA / GUICHET UNIQUE | No | — |
| 96 | Armenia | AM | e-register.am | Yes | — |
| 97 | Cyprus | CY | Registrar of Companies | Bulk DL | — |
| 98 | Honduras | HN | Registro Mercantil | No | — |
| 99 | Iceland | IS | Hlutafélagaskrá (RSK) | Yes | — |
| 100 | Bosnia & Herzegovina | BA | APIF / JPRS | Partial | — |
