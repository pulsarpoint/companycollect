# BRREG Financial Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Go HTTP service at `data-pipelines/services/brreg-financial-service/` that fetches BRREG Regnskapsregister key figures by organization number, normalizes them into typed decimal-string fields, and returns per-record statuses for batched Temporal activity use.

**Architecture:** Mirrors `data-pipelines/services/currency-service` exactly — stdlib mux, `getEnv` config, graceful shutdown, `GOWORK=off`. Five internal packages: `models` (shared types with JSON tags), `parser` (pure JSON→ParsedRecord, no I/O), `brregclient` (HTTP to data.brreg.no), `service` (orchestrates brregclient+parser, builds LookupResponse), `httpapi` (thin HTTP handler). Tests use `httptest.Server` for brregclient and a stub interface for service/handler tests.

**Tech Stack:** Go 1.26.1, `shopspring/decimal v1.4.0`, `stretchr/testify v1.11.1`, stdlib (`crypto/sha256`, `net/http`, `encoding/json`).

---

## File Map

```
data-pipelines/services/brreg-financial-service/
├── cmd/brreg-financial-service/main.go
├── internal/
│   ├── models/models.go
│   ├── parser/
│   │   ├── parser.go
│   │   ├── parser_test.go
│   │   └── testdata/
│   │       ├── equinor_list.json        # 923609016 USD large IFRS-simplified
│   │       ├── akerbp_list.json         # 989795848 USD full-IFRS + totalresultat
│   │       ├── banenor_list.json        # 917082308 NOK standard rules SF org
│   │       ├── bortigard_list.json      # 810202572 NOK smaaForetak=true
│   │       ├── nel_list.json            # 915501680 NOK negative earnings
│   │       ├── mowi_list.json           # 964118191 EUR
│   │       ├── dnb_500.json             # 984851006 HTTP 500 unsupported BANK plan
│   │       ├── storebrand_500.json      # 930553506 HTTP 500 unsupported SKADE plan
│   │       ├── equinor_pdf_years.json   # PDF years list (14 entries)
│   │       ├── dnb_pdf_years.json       # PDF years for unsupported-plan org
│   │       ├── konsern_list.json        # synthetic: regnskapstype=KONSERN
│   │       ├── no_revenue_list.json     # synthetic: all financial fields absent
│   │       ├── audit_optout_list.json   # synthetic: fravalgRevisjon=true
│   │       └── liquidation_list.json   # synthetic: avviklingsregnskap=true
│   ├── brregclient/
│   │   ├── client.go
│   │   └── client_test.go
│   ├── service/
│   │   ├── service.go
│   │   └── service_test.go
│   └── httpapi/
│       ├── handler.go
│       ├── types.go
│       └── handler_test.go
├── go.mod
├── Makefile
└── Dockerfile
```

---

## Task 1: Scaffold — directories, go.mod, Makefile, Dockerfile

**Files:**
- Create: `data-pipelines/services/brreg-financial-service/go.mod`
- Create: `data-pipelines/services/brreg-financial-service/Makefile`
- Create: `data-pipelines/services/brreg-financial-service/Dockerfile`

- [ ] **Step 1: Create directories**

```bash
cd data-pipelines/services
mkdir -p brreg-financial-service/{cmd/brreg-financial-service,internal/{models,parser/testdata,brregclient,service,httpapi}}
```

- [ ] **Step 2: Write `brreg-financial-service/go.mod`**

```
module github.com/pulsarpoint/brreg-financial-service

go 1.26.1

require (
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.11.1
)
```

- [ ] **Step 3: Write `brreg-financial-service/Makefile`**

```makefile
.PHONY: build test run down logs

build:
	GOWORK=off go build -o bin/brreg-financial-service ./cmd/brreg-financial-service

test:
	GOWORK=off go test ./...

run:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
```

- [ ] **Step 4: Write `brreg-financial-service/Dockerfile`**

```dockerfile
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOWORK=off CGO_ENABLED=0 go build -o /brreg-financial-service ./cmd/brreg-financial-service

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /brreg-financial-service /brreg-financial-service
ENTRYPOINT ["/brreg-financial-service"]
```

- [ ] **Step 5: Create placeholder package files so `go mod tidy` compiles**

`cmd/brreg-financial-service/main.go`:
```go
package main

func main() {}
```

`internal/models/models.go`: `package models`
`internal/parser/parser.go`: `package parser`
`internal/brregclient/client.go`: `package brregclient`
`internal/service/service.go`: `package service`
`internal/httpapi/handler.go`: `package httpapi`

- [ ] **Step 6: Generate go.sum**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go mod tidy
```

Expected: `go.sum` created, no errors.

- [ ] **Step 7: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/
git commit -m "feat: scaffold brreg-financial-service go module"
```

---

## Task 2: Real fixture files

**Files:** All files under `internal/parser/testdata/`

- [ ] **Step 1: Write `testdata/equinor_list.json`** (org 923609016, USD, IFRS-simplified, id=5667197)

```json
[
    {
        "id": 5667197,
        "journalnr": "2025428073",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "923609016",
            "organisasjonsform": "ASA",
            "morselskap": true
        },
        "regnskapsperiode": {
            "fraDato": "2024-01-01",
            "tilDato": "2024-12-31"
        },
        "valuta": "USD",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {
            "ikkeRevidertAarsregnskap": false,
            "fravalgRevisjon": false
        },
        "regnkapsprinsipper": {
            "smaaForetak": false,
            "regnskapsregler": "forenkletAnvendelseIFRS"
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 109150000000.0,
            "egenkapital": {
                "sumEgenkapital": 41090000000.0,
                "opptjentEgenkapital": {"sumOpptjentEgenkapital": 40038000000.0},
                "innskuttEgenkapital": {"sumInnskuttEgenkaptial": 1052000000.0}
            },
            "gjeldOversikt": {
                "sumGjeld": 68060000000.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 42024000000.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 26036000000.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 109150000000.0,
            "omloepsmidler": {"sumOmloepsmidler": 45079000000.0},
            "anleggsmidler": {"sumAnleggsmidler": 64071000000.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 8168000000.0,
            "aarsresultat": 8141000000.0,
            "finansresultat": {
                "nettoFinans": -2179000000.0,
                "finansinntekt": {"sumFinansinntekter": 516000000.0},
                "finanskostnad": {"sumFinanskostnad": 2695000000.0}
            },
            "driftsresultat": {
                "driftsresultat": 10347000000.0,
                "driftsinntekter": {"sumDriftsinntekter": 72543000000.0},
                "driftskostnad": {"sumDriftskostnad": 62196000000.0}
            }
        }
    }
]
```

- [ ] **Step 2: Write `testdata/akerbp_list.json`** (org 989795848, USD, full IFRS, has `totalresultat`)

```json
[
    {
        "id": 6252097,
        "journalnr": "2025597111",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "989795848",
            "organisasjonsform": "ASA",
            "morselskap": true
        },
        "regnskapsperiode": {
            "fraDato": "2024-01-01",
            "tilDato": "2024-12-31"
        },
        "valuta": "USD",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {
            "ikkeRevidertAarsregnskap": false,
            "fravalgRevisjon": false
        },
        "regnkapsprinsipper": {
            "smaaForetak": false,
            "regnskapsregler": "IFRS"
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 42195000000.0,
            "egenkapital": {
                "sumEgenkapital": 12691000000.0,
                "opptjentEgenkapital": {"sumOpptjentEgenkapital": -340000000.0},
                "innskuttEgenkapital": {"sumInnskuttEgenkaptial": 13031000000.0}
            },
            "gjeldOversikt": {
                "sumGjeld": 29504000000.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 4516000000.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 24988000000.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 42193000000.0,
            "omloepsmidler": {"sumOmloepsmidler": 6164000000.0},
            "anleggsmidler": {"sumAnleggsmidler": 36029000000.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 8039000000.0,
            "aarsresultat": 1818000000.0,
            "totalresultat": 1818000000.0,
            "finansresultat": {
                "nettoFinans": -225000000.0,
                "finansinntekt": {"sumFinansinntekter": 555000000.0},
                "finanskostnad": {"sumFinanskostnad": 780000000.0}
            },
            "driftsresultat": {
                "driftsresultat": 8264000000.0,
                "driftsinntekter": {"sumDriftsinntekter": 12380000000.0},
                "driftskostnad": {"sumDriftskostnad": 4116000000.0}
            }
        }
    }
]
```

- [ ] **Step 3: Write `testdata/banenor_list.json`** (org 917082308, NOK, SF org form, operating loss)

```json
[
    {
        "id": 6300609,
        "journalnr": "2025741889",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "917082308",
            "organisasjonsform": "SF",
            "morselskap": true
        },
        "regnskapsperiode": {
            "fraDato": "2024-01-01",
            "tilDato": "2024-12-31"
        },
        "valuta": "NOK",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {
            "ikkeRevidertAarsregnskap": false,
            "fravalgRevisjon": false
        },
        "regnkapsprinsipper": {
            "smaaForetak": false,
            "regnskapsregler": "regnskapslovenAlminneligRegler"
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 245683000000.0,
            "egenkapital": {
                "sumEgenkapital": 8678000000.0,
                "opptjentEgenkapital": {"sumOpptjentEgenkapital": -331000000.0},
                "innskuttEgenkapital": {"sumInnskuttEgenkaptial": 9009000000.0}
            },
            "gjeldOversikt": {
                "sumGjeld": 237005000000.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 12239000000.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 224766000000.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 245683000000.0,
            "omloepsmidler": {"sumOmloepsmidler": 11157000000.0},
            "anleggsmidler": {"sumAnleggsmidler": 234526000000.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 758000000.0,
            "aarsresultat": 752000000.0,
            "finansresultat": {
                "nettoFinans": 773000000.0,
                "finansinntekt": {"sumFinansinntekter": 813000000.0},
                "finanskostnad": {"sumFinanskostnad": 40000000.0}
            },
            "driftsresultat": {
                "driftsresultat": -15000000.0,
                "driftsinntekter": {"sumDriftsinntekter": 17763000000.0},
                "driftskostnad": {"sumDriftskostnad": 17778000000.0}
            }
        }
    }
]
```

- [ ] **Step 4: Write `testdata/bortigard_list.json`** (org 810202572, NOK, smaaForetak=true, small amounts)

```json
[
    {
        "id": 6193159,
        "journalnr": "2025694978",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "810202572",
            "organisasjonsform": "AS",
            "morselskap": true
        },
        "regnskapsperiode": {
            "fraDato": "2024-01-01",
            "tilDato": "2024-12-31"
        },
        "valuta": "NOK",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {
            "ikkeRevidertAarsregnskap": false,
            "fravalgRevisjon": false
        },
        "regnkapsprinsipper": {
            "smaaForetak": true,
            "regnskapsregler": "regnskapslovenAlminneligRegler"
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 6059747.0,
            "egenkapital": {
                "sumEgenkapital": 5957114.0,
                "opptjentEgenkapital": {"sumOpptjentEgenkapital": 5769133.0},
                "innskuttEgenkapital": {"sumInnskuttEgenkaptial": 187981.0}
            },
            "gjeldOversikt": {
                "sumGjeld": 102633.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 78465.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 24168.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 6059747.0,
            "omloepsmidler": {"sumOmloepsmidler": 3086673.0},
            "anleggsmidler": {"sumAnleggsmidler": 2973074.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 197030.0,
            "aarsresultat": 186155.0,
            "finansresultat": {
                "nettoFinans": 205629.0,
                "finansinntekt": {"sumFinansinntekter": 455732.0},
                "finanskostnad": {"sumFinanskostnad": 250103.0}
            },
            "driftsresultat": {
                "driftsresultat": -8599.0,
                "driftsinntekter": {"sumDriftsinntekter": 174012.0},
                "driftskostnad": {"sumDriftskostnad": 182611.0}
            }
        }
    }
]
```

- [ ] **Step 5: Write `testdata/nel_list.json`** (org 915501680, NOK, negative earnings)

```json
[
    {
        "id": 6222227,
        "journalnr": "2025585345",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "915501680",
            "organisasjonsform": "ASA",
            "morselskap": true
        },
        "regnskapsperiode": {
            "fraDato": "2024-01-01",
            "tilDato": "2024-12-31"
        },
        "valuta": "NOK",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {
            "ikkeRevidertAarsregnskap": false,
            "fravalgRevisjon": false
        },
        "regnkapsprinsipper": {
            "smaaForetak": false,
            "regnskapsregler": "regnskapslovenAlminneligRegler"
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 1380126000.0,
            "egenkapital": {
                "sumEgenkapital": 1313849000.0,
                "opptjentEgenkapital": {"sumOpptjentEgenkapital": -618588000.0},
                "innskuttEgenkapital": {"sumInnskuttEgenkaptial": 1932437000.0}
            },
            "gjeldOversikt": {
                "sumGjeld": 66277000.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 60847000.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 5430000.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 1380128000.0,
            "omloepsmidler": {"sumOmloepsmidler": 113931000.0},
            "anleggsmidler": {"sumAnleggsmidler": 1266197000.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": -260742000.0,
            "aarsresultat": -260874000.0,
            "finansresultat": {
                "nettoFinans": -177592000.0,
                "finansinntekt": {"sumFinansinntekter": 51946000.0},
                "finanskostnad": {"sumFinanskostnad": 229538000.0}
            },
            "driftsresultat": {
                "driftsresultat": -83150000.0,
                "driftsinntekter": {"sumDriftsinntekter": 90624000.0},
                "driftskostnad": {"sumDriftskostnad": 173774000.0}
            }
        }
    }
]
```

- [ ] **Step 6: Write `testdata/mowi_list.json`** (org 964118191, EUR)

```json
[
    {
        "id": 6300613,
        "journalnr": "2025741982",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "964118191",
            "organisasjonsform": "ASA",
            "morselskap": true
        },
        "regnskapsperiode": {
            "fraDato": "2024-01-01",
            "tilDato": "2024-12-31"
        },
        "valuta": "EUR",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {
            "ikkeRevidertAarsregnskap": false,
            "fravalgRevisjon": false
        },
        "regnkapsprinsipper": {
            "smaaForetak": false,
            "regnskapsregler": "regnskapslovenAlminneligRegler"
        },
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 6440000000.0,
            "egenkapital": {
                "sumEgenkapital": 2919000000.0,
                "opptjentEgenkapital": {"sumOpptjentEgenkapital": 1239000000.0},
                "innskuttEgenkapital": {"sumInnskuttEgenkaptial": 1680000000.0}
            },
            "gjeldOversikt": {
                "sumGjeld": 3521000000.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 1666000000.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 1855000000.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 6440000000.0,
            "omloepsmidler": {"sumOmloepsmidler": 1591000000.0},
            "anleggsmidler": {"sumAnleggsmidler": 4849000000.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 238000000.0,
            "aarsresultat": 194000000.0,
            "finansresultat": {
                "nettoFinans": -29000000.0,
                "finansinntekt": {"sumFinansinntekter": 135000000.0},
                "finanskostnad": {"sumFinanskostnad": 164000000.0}
            },
            "driftsresultat": {
                "driftsresultat": 267000000.0,
                "driftsinntekter": {"sumDriftsinntekter": 1931000000.0},
                "driftskostnad": {"sumDriftskostnad": 1664000000.0}
            }
        }
    }
]
```

- [ ] **Step 7: Write `testdata/dnb_500.json`** (HTTP 500 unsupported BANK plan body)

```json
{
    "timestamp": "2026-05-31T11:15:50.815+0000",
    "status": "500",
    "error": "Internal Server Error",
    "message": "Regnskapet inneholder en oppstillingsplan som ikke er stottet (BANK)",
    "path": "/regnskapsregisteret/regnskap/984851006",
    "trace": "5f9df86f-57b8-4455-8451-57b9d0a808aa"
}
```

- [ ] **Step 8: Write `testdata/storebrand_500.json`** (HTTP 500 unsupported SKADE plan body)

```json
{
    "timestamp": "2026-05-31T11:16:13.116+0000",
    "status": "500",
    "error": "Internal Server Error",
    "message": "Regnskapet inneholder en oppstillingsplan som ikke er stottet (SKADE)",
    "path": "/regnskapsregisteret/regnskap/930553506",
    "trace": "f83094f1-3011-40a3-807d-267c325b6fe3"
}
```

- [ ] **Step 9: Write `testdata/equinor_pdf_years.json`**

```json
["2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"]
```

- [ ] **Step 10: Write `testdata/dnb_pdf_years.json`** (DNB has PDFs even though JSON plan is unsupported)

```json
["2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024","2025"]
```

- [ ] **Step 11: Write `testdata/konsern_list.json`** (synthetic: KONSERN → "group")

Copy `equinor_list.json` but set `"regnskapstype": "KONSERN"` and `"id": 9000001`.

```json
[
    {
        "id": 9000001,
        "journalnr": "SYNTHETIC001",
        "regnskapstype": "KONSERN",
        "virksomhet": {
            "organisasjonsnummer": "923609016",
            "organisasjonsform": "ASA",
            "morselskap": true
        },
        "regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"},
        "valuta": "USD",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": false, "fravalgRevisjon": false},
        "regnkapsprinsipper": {"smaaForetak": false, "regnskapsregler": "IFRS"},
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 50000000.0,
            "egenkapital": {"sumEgenkapital": 30000000.0},
            "gjeldOversikt": {
                "sumGjeld": 20000000.0,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 10000000.0},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 10000000.0}
            }
        },
        "eiendeler": {
            "sumEiendeler": 50000000.0,
            "omloepsmidler": {"sumOmloepsmidler": 20000000.0},
            "anleggsmidler": {"sumAnleggsmidler": 30000000.0}
        },
        "resultatregnskapResultat": {
            "ordinaertResultatFoerSkattekostnad": 5000000.0,
            "aarsresultat": 4000000.0,
            "driftsresultat": {
                "driftsresultat": 6000000.0,
                "driftsinntekter": {"sumDriftsinntekter": 100000000.0}
            }
        }
    }
]
```

- [ ] **Step 12: Write `testdata/no_revenue_list.json`** (synthetic: missing all financial sub-objects)

```json
[
    {
        "id": 9000002,
        "journalnr": "SYNTHETIC002",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "999999999",
            "organisasjonsform": "AS",
            "morselskap": false
        },
        "regnskapsperiode": {"fraDato": "2023-01-01", "tilDato": "2023-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": false, "fravalgRevisjon": false},
        "regnkapsprinsipper": {"smaaForetak": true, "regnskapsregler": "regnskapslovenAlminneligRegler"}
    }
]
```

- [ ] **Step 13: Write `testdata/audit_optout_list.json`** (synthetic: fravalgRevisjon=true)

```json
[
    {
        "id": 9000003,
        "journalnr": "SYNTHETIC003",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "888888888",
            "organisasjonsform": "AS",
            "morselskap": false
        },
        "regnskapsperiode": {"fraDato": "2023-01-01", "tilDato": "2023-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": false,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": false, "fravalgRevisjon": true},
        "regnkapsprinsipper": {"smaaForetak": true, "regnskapsregler": "regnskapslovenAlminneligRegler"},
        "eiendeler": {"sumEiendeler": 500000.0},
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 500000.0,
            "egenkapital": {"sumEgenkapital": 400000.0},
            "gjeldOversikt": {"sumGjeld": 100000.0}
        },
        "resultatregnskapResultat": {
            "aarsresultat": 50000.0,
            "driftsresultat": {
                "driftsresultat": 50000.0,
                "driftsinntekter": {"sumDriftsinntekter": 200000.0}
            }
        }
    }
]
```

- [ ] **Step 14: Write `testdata/liquidation_list.json`** (synthetic: avviklingsregnskap=true)

```json
[
    {
        "id": 9000004,
        "journalnr": "SYNTHETIC004",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "777777777",
            "organisasjonsform": "AS",
            "morselskap": false
        },
        "regnskapsperiode": {"fraDato": "2023-01-01", "tilDato": "2023-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": true,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": false, "fravalgRevisjon": false},
        "regnkapsprinsipper": {"smaaForetak": false, "regnskapsregler": "regnskapslovenAlminneligRegler"},
        "eiendeler": {"sumEiendeler": 100000.0},
        "egenkapitalGjeld": {
            "sumEgenkapitalGjeld": 100000.0,
            "egenkapital": {"sumEgenkapital": 80000.0},
            "gjeldOversikt": {"sumGjeld": 20000.0}
        },
        "resultatregnskapResultat": {
            "aarsresultat": -10000.0,
            "driftsresultat": {
                "driftsresultat": -10000.0,
                "driftsinntekter": {"sumDriftsinntekter": 0.0}
            }
        }
    }
]
```

- [ ] **Step 15: Commit fixtures**

```bash
git add data-pipelines/services/brreg-financial-service/internal/parser/testdata/
git commit -m "feat: add brreg-financial-service parser test fixtures"
```

---

## Task 3: models package

**Files:**
- Modify: `data-pipelines/services/brreg-financial-service/internal/models/models.go`

- [ ] **Step 1: Write `internal/models/models.go`**

```go
package models

import "encoding/json"

const SchemaVersion = "brreg-financial-service.lookup.v1"

// LookupRecord is one organization in a batch request.
type LookupRecord struct {
	RecordID             string `json:"record_id"`
	OrganizationNumber   string `json:"organization_number"`
	OrganizationName     string `json:"organization_name,omitempty"`
	LastAnnualReportYear int    `json:"last_annual_report_year,omitempty"`
}

// LookupRequest is the JSON body for POST /v1/brreg/financials/lookup.
type LookupRequest struct {
	Records            []LookupRecord `json:"records"`
	IncludePDFMetadata bool           `json:"include_pdf_metadata"`
	IncludeRawPayload  bool           `json:"include_raw_payload"`
}

// LookupResponse is the JSON body for POST /v1/brreg/financials/lookup response.
type LookupResponse struct {
	SchemaVersion    string         `json:"schema_version"`
	Status           string         `json:"status"`
	RecordsSeen      int            `json:"records_seen"`
	RecordsCompleted int            `json:"records_completed"`
	RecordsFailed    int            `json:"records_failed"`
	DurationMs       int64          `json:"duration_ms"`
	Results          []RecordResult `json:"results"`
}

// RecordResult is the outcome for one organization in the batch.
// Status: succeeded | not_available | unsupported_statement_plan | failed
type RecordResult struct {
	RecordID           string       `json:"record_id"`
	OrganizationNumber string       `json:"organization_number"`
	Status             string       `json:"status"`
	Statements         []Statement  `json:"statements"`
	PDFMetadata        *PDFMetadata `json:"pdf_metadata,omitempty"`
	Warnings           []Warning    `json:"warnings"`
}

// Statement is one normalized BRREG annual-account key-figure record.
// All amount fields are decimal strings (e.g. "72543000000.00") or null.
type Statement struct {
	SourceRecordID                     string            `json:"source_record_id"`
	JournalNumber                      string            `json:"journal_number"`
	FiscalYear                         int               `json:"fiscal_year"`
	PeriodStart                        string            `json:"period_start"`
	PeriodEnd                          string            `json:"period_end"`
	StatementType                      string            `json:"statement_type"`
	OriginalCurrency                   string            `json:"original_currency"`
	RevenueOriginalAmount              *string           `json:"revenue_original_amount"`
	SalesRevenueOriginalAmount         *string           `json:"sales_revenue_original_amount"`
	OperatingProfitOriginalAmount      *string           `json:"operating_profit_original_amount"`
	ProfitBeforeTaxOriginalAmount      *string           `json:"profit_before_tax_original_amount"`
	TaxExpenseOriginalAmount           *string           `json:"tax_expense_original_amount"`
	NetIncomeOriginalAmount            *string           `json:"net_income_original_amount"`
	TotalResultOriginalAmount          *string           `json:"total_result_original_amount"`
	TotalAssetsOriginalAmount          *string           `json:"total_assets_original_amount"`
	CurrentAssetsOriginalAmount        *string           `json:"current_assets_original_amount"`
	FixedAssetsOriginalAmount          *string           `json:"fixed_assets_original_amount"`
	TotalEquityOriginalAmount          *string           `json:"total_equity_original_amount"`
	TotalLiabilitiesOriginalAmount     *string           `json:"total_liabilities_original_amount"`
	ShortTermLiabilitiesOriginalAmount *string           `json:"short_term_liabilities_original_amount"`
	LongTermLiabilitiesOriginalAmount  *string           `json:"long_term_liabilities_original_amount"`
	Facts                              map[string]string `json:"facts,omitempty"`
	Metadata                           StatementMetadata `json:"metadata"`
	Evidence                           StatementEvidence `json:"evidence"`
	RawPayload                         json.RawMessage   `json:"raw_payload,omitempty"`
}

// StatementMetadata contains descriptive metadata about the statement.
type StatementMetadata struct {
	OrganizationForm    string `json:"organization_form"`
	IsParentCompany     bool   `json:"is_parent_company"`
	StatementPlan       string `json:"statement_plan"`
	AccountingRules     string `json:"accounting_rules"`
	SmallCompany        bool   `json:"small_company"`
	NotAudited          bool   `json:"not_audited"`
	AuditOptOut         bool   `json:"audit_opt_out"`
	LiquidationAccounts bool   `json:"liquidation_accounts"`
}

// StatementEvidence records where data came from and how to verify it.
type StatementEvidence struct {
	Source         string `json:"source"`
	SourceURL      string `json:"source_url"`
	DetailURL      string `json:"detail_url"`
	RawPayloadHash string `json:"raw_payload_hash"`
}

// PDFMetadata describes available annual-account PDFs.
type PDFMetadata struct {
	AvailableYears      []string `json:"available_years"`
	DownloadURLTemplate string   `json:"download_url_template"`
}

// Warning is a non-fatal diagnostic for a record result.
type Warning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Detail  map[string]any `json:"detail,omitempty"`
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go build ./internal/models/...
```

Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/internal/models/
git commit -m "feat: add brreg-financial-service models package"
```

---

## Task 4: parser package (TDD)

**Files:**
- Modify: `internal/parser/parser.go`
- Create: `internal/parser/parser_test.go`

The parser has no I/O. It decodes raw BRREG JSON bytes into `models.Statement` slices.
It also parses BRREG 500 error bodies to detect unsupported plan names.

- [ ] **Step 1: Write `internal/parser/parser_test.go`**

```go
package parser_test

import (
	"os"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/parser"
	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

// --- ParseKeyFigures ---

func TestParseKeyFigures_Equinor(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "equinor_list.json"), "923609016", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)

	s := records[0]
	require.Equal(t, "5667197", s.SourceRecordID)
	require.Equal(t, "2025428073", s.JournalNumber)
	require.Equal(t, 2024, s.FiscalYear)
	require.Equal(t, "2024-01-01", s.PeriodStart)
	require.Equal(t, "2024-12-31", s.PeriodEnd)
	require.Equal(t, "company", s.StatementType)
	require.Equal(t, "USD", s.OriginalCurrency)

	// amounts preserved as decimal strings, 2dp
	require.NotNil(t, s.RevenueOriginalAmount)
	require.Equal(t, "72543000000.00", *s.RevenueOriginalAmount)
	require.Nil(t, s.SalesRevenueOriginalAmount)   // not present in source
	require.Nil(t, s.TotalResultOriginalAmount)    // not present in source

	require.NotNil(t, s.OperatingProfitOriginalAmount)
	require.Equal(t, "10347000000.00", *s.OperatingProfitOriginalAmount)

	require.NotNil(t, s.TotalAssetsOriginalAmount)
	require.Equal(t, "109150000000.00", *s.TotalAssetsOriginalAmount)

	require.NotNil(t, s.TotalEquityOriginalAmount)
	require.Equal(t, "41090000000.00", *s.TotalEquityOriginalAmount)

	// facts
	require.Contains(t, s.Facts, "finance_result_original_amount")
	require.Equal(t, "-2179000000.00", s.Facts["finance_result_original_amount"])
	require.Equal(t, "516000000.00", s.Facts["financial_income_original_amount"])
	require.Equal(t, "2695000000.00", s.Facts["financial_cost_original_amount"])

	// metadata
	require.Equal(t, "ASA", s.Metadata.OrganizationForm)
	require.True(t, s.Metadata.IsParentCompany)
	require.Equal(t, "store", s.Metadata.StatementPlan)
	require.Equal(t, "forenkletAnvendelseIFRS", s.Metadata.AccountingRules)
	require.False(t, s.Metadata.SmallCompany)
	require.False(t, s.Metadata.LiquidationAccounts)

	// evidence
	require.Equal(t, "brreg_regnskapsregisteret", s.Evidence.Source)
	require.Equal(t, "https://data.brreg.no/regnskapsregisteret/regnskap/923609016", s.Evidence.SourceURL)
	require.Equal(t, "https://data.brreg.no/regnskapsregisteret/regnskap/923609016/5667197", s.Evidence.DetailURL)
	require.NotEmpty(t, s.Evidence.RawPayloadHash)
	require.Contains(t, s.Evidence.RawPayloadHash, "sha256:")
}

func TestParseKeyFigures_AkerBP_HasTotalresultat(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "akerbp_list.json"), "989795848", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.NotNil(t, s.TotalResultOriginalAmount)
	require.Equal(t, "1818000000.00", *s.TotalResultOriginalAmount)
	require.Equal(t, "USD", s.OriginalCurrency)
	require.Equal(t, "IFRS", s.Metadata.AccountingRules)
}

func TestParseKeyFigures_BaneNor_NegativeOperatingProfit(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "banenor_list.json"), "917082308", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.Equal(t, "NOK", s.OriginalCurrency)
	require.Equal(t, "SF", s.Metadata.OrganizationForm)
	require.NotNil(t, s.OperatingProfitOriginalAmount)
	require.Equal(t, "-15000000.00", *s.OperatingProfitOriginalAmount)
}

func TestParseKeyFigures_Bortigard_SmallCompany(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "bortigard_list.json"), "810202572", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.True(t, s.Metadata.SmallCompany)
	// small amounts should still round-trip correctly
	require.Equal(t, "174012.00", *s.RevenueOriginalAmount)
	require.Equal(t, "6059747.00", *s.TotalAssetsOriginalAmount)
}

func TestParseKeyFigures_Nel_NegativeNetIncome(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "nel_list.json"), "915501680", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.NotNil(t, s.NetIncomeOriginalAmount)
	require.Equal(t, "-260874000.00", *s.NetIncomeOriginalAmount)
	require.NotNil(t, s.ProfitBeforeTaxOriginalAmount)
	require.Equal(t, "-260742000.00", *s.ProfitBeforeTaxOriginalAmount)
}

func TestParseKeyFigures_Mowi_EUR(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "mowi_list.json"), "964118191", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "EUR", records[0].OriginalCurrency)
}

func TestParseKeyFigures_Konsern_StatementTypeGroup(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "konsern_list.json"), "923609016", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "group", records[0].StatementType)
}

func TestParseKeyFigures_NoRevenue_AllNull(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "no_revenue_list.json"), "999999999", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.Nil(t, s.RevenueOriginalAmount)
	require.Nil(t, s.OperatingProfitOriginalAmount)
	require.Nil(t, s.TotalAssetsOriginalAmount)
	require.Empty(t, s.Facts)
}

func TestParseKeyFigures_AuditOptOut(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "audit_optout_list.json"), "888888888", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, records[0].Metadata.AuditOptOut)
	require.False(t, records[0].Metadata.NotAudited)
}

func TestParseKeyFigures_Liquidation(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "liquidation_list.json"), "777777777", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, records[0].Metadata.LiquidationAccounts)
}

func TestParseKeyFigures_EmptyArray(t *testing.T) {
	records, err := parser.ParseKeyFigures([]byte("[]"), "000000000", "https://data.brreg.no")
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestParseKeyFigures_RawPayloadHash_Deterministic(t *testing.T) {
	data := fixture(t, "equinor_list.json")
	r1, _ := parser.ParseKeyFigures(data, "923609016", "https://data.brreg.no")
	r2, _ := parser.ParseKeyFigures(data, "923609016", "https://data.brreg.no")
	require.Equal(t, r1[0].Evidence.RawPayloadHash, r2[0].Evidence.RawPayloadHash)
}

// --- ParseUnsupportedPlan ---

func TestParseUnsupportedPlan_BANK(t *testing.T) {
	plan, ok := parser.ParseUnsupportedPlan(fixture(t, "dnb_500.json"))
	require.True(t, ok)
	require.Equal(t, "BANK", plan)
}

func TestParseUnsupportedPlan_SKADE(t *testing.T) {
	plan, ok := parser.ParseUnsupportedPlan(fixture(t, "storebrand_500.json"))
	require.True(t, ok)
	require.Equal(t, "SKADE", plan)
}

func TestParseUnsupportedPlan_GenericError(t *testing.T) {
	body := []byte(`{"status":"500","message":"internal server error"}`)
	_, ok := parser.ParseUnsupportedPlan(body)
	require.False(t, ok)
}

// --- ParsePDFYears ---

func TestParsePDFYears_Equinor(t *testing.T) {
	years, err := parser.ParsePDFYears(fixture(t, "equinor_pdf_years.json"))
	require.NoError(t, err)
	require.Len(t, years, 14)
	require.Equal(t, "2011", years[0])
	require.Equal(t, "2024", years[13])
}

func TestParsePDFYears_DNB(t *testing.T) {
	years, err := parser.ParsePDFYears(fixture(t, "dnb_pdf_years.json"))
	require.NoError(t, err)
	require.Len(t, years, 15)
}

func TestParsePDFYears_Empty(t *testing.T) {
	years, err := parser.ParsePDFYears([]byte("[]"))
	require.NoError(t, err)
	require.Empty(t, years)
}
```

- [ ] **Step 2: Run tests — confirm they all fail with "undefined"**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/parser/...
```

Expected: compilation errors — `parser.ParseKeyFigures`, `parser.ParseUnsupportedPlan`, `parser.ParsePDFYears` undefined.

- [ ] **Step 3: Write `internal/parser/parser.go`**

```go
package parser

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/shopspring/decimal"
)

// ParseKeyFigures parses the raw BRREG list-endpoint response body.
// orgNum and baseURL are used to build evidence URLs.
// Returns an empty slice (no error) for a valid empty array.
func ParseKeyFigures(raw []byte, orgNum, baseURL string) ([]models.Statement, error) {
	var rawRecords []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&rawRecords); err != nil {
		return nil, fmt.Errorf("decode brreg key figures: %w", err)
	}
	out := make([]models.Statement, 0, len(rawRecords))
	for _, raw := range rawRecords {
		rec, err := decodeRecord(raw)
		if err != nil {
			return nil, fmt.Errorf("decode brreg record: %w", err)
		}
		out = append(out, toStatement(rec, raw, orgNum, baseURL))
	}
	return out, nil
}

// ParseUnsupportedPlan inspects a BRREG HTTP 500 response body.
// Returns (planName, true) when the error indicates an unsupported statement plan,
// e.g. "Regnskapet inneholder en oppstillingsplan som ikke er stottet (BANK)".
func ParseUnsupportedPlan(body []byte) (string, bool) {
	var resp struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", false
	}
	const marker = "Regnskapet inneholder en oppstillingsplan som ikke er stottet ("
	idx := strings.Index(resp.Message, marker)
	if idx < 0 {
		return "", false
	}
	rest := resp.Message[idx+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

// ParsePDFYears parses the BRREG PDF-years list endpoint response body.
// Returns a slice of year strings, e.g. ["2011","2012",...].
func ParsePDFYears(raw []byte) ([]string, error) {
	var years []string
	if err := json.Unmarshal(raw, &years); err != nil {
		return nil, fmt.Errorf("decode pdf years: %w", err)
	}
	return years, nil
}

// --- internal types matching the BRREG key-figure JSON schema ---

type brregRecord struct {
	ID            json.Number `json:"id"`
	JournalNr     string      `json:"journalnr"`
	Regnskapstype string      `json:"regnskapstype"`
	Virksomhet    struct {
		Organisasjonsnummer string `json:"organisasjonsnummer"`
		Organisasjonsform   string `json:"organisasjonsform"`
		Morselskap          bool   `json:"morselskap"`
	} `json:"virksomhet"`
	Regnskapsperiode struct {
		FraDato string `json:"fraDato"`
		TilDato string `json:"tilDato"`
	} `json:"regnskapsperiode"`
	Valuta             string `json:"valuta"`
	Avviklingsregnskap bool   `json:"avviklingsregnskap"`
	Oppstillingsplan   string `json:"oppstillingsplan"`
	Revisjon           struct {
		IkkeRevidertAarsregnskap bool `json:"ikkeRevidertAarsregnskap"`
		FravalgRevisjon          bool `json:"fravalgRevisjon"`
	} `json:"revisjon"`
	Regnkapsprinsipper struct {
		SmaaForetak     bool   `json:"smaaForetak"`
		Regnskapsregler string `json:"regnskapsregler"`
	} `json:"regnkapsprinsipper"`
	Resultatregnskap *brregResultat        `json:"resultatregnskapResultat"`
	Eiendeler        *brregEiendeler       `json:"eiendeler"`
	EgKapGjeld       *brregEgenkapGjeld    `json:"egenkapitalGjeld"`
}

type brregResultat struct {
	OrdinaertFoerSkatt json.Number    `json:"ordinaertResultatFoerSkattekostnad"`
	Skattekostnad      json.Number    `json:"ordinaertResultatSkattekostnad"`
	Aarsresultat       json.Number    `json:"aarsresultat"`
	Totalresultat      json.Number    `json:"totalresultat"`
	Finansresultat     *brregFinans   `json:"finansresultat"`
	Driftsresultat     *brregDrift    `json:"driftsresultat"`
}

type brregFinans struct {
	NettoFinans   json.Number `json:"nettoFinans"`
	Finansinntekt *struct {
		Sum json.Number `json:"sumFinansinntekter"`
	} `json:"finansinntekt"`
	Finanskostnad *struct {
		Sum json.Number `json:"sumFinanskostnad"`
	} `json:"finanskostnad"`
}

type brregDrift struct {
	Driftsresultat  json.Number   `json:"driftsresultat"`
	Driftsinntekter *brregInntekt `json:"driftsinntekter"`
}

type brregInntekt struct {
	Sum            json.Number `json:"sumDriftsinntekter"`
	Salgsinntekter json.Number `json:"salgsinntekter"`
}

type brregEiendeler struct {
	Sum         json.Number `json:"sumEiendeler"`
	Omloep      *struct{ Sum json.Number `json:"sumOmloepsmidler"` } `json:"omloepsmidler"`
	Anlegg      *struct{ Sum json.Number `json:"sumAnleggsmidler"` } `json:"anleggsmidler"`
}

type brregEgenkapGjeld struct {
	Egenkapital *struct {
		Sum json.Number `json:"sumEgenkapital"`
	} `json:"egenkapital"`
	GjeldOversikt *struct {
		Sum         json.Number `json:"sumGjeld"`
		Kortsiktig  *struct{ Sum json.Number `json:"sumKortsiktigGjeld"` }  `json:"kortsiktigGjeld"`
		Langsiktig  *struct{ Sum json.Number `json:"sumLangsiktigGjeld"` }  `json:"langsiktigGjeld"`
	} `json:"gjeldOversikt"`
}

func decodeRecord(raw json.RawMessage) (brregRecord, error) {
	var rec brregRecord
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return rec, dec.Decode(&rec)
}

func toStatement(rec brregRecord, raw json.RawMessage, orgNum, baseURL string) models.Statement {
	hash := sha256.Sum256(raw)
	hashStr := "sha256:" + hex.EncodeToString(hash[:])

	fiscalYear := extractFiscalYear(rec.Regnskapsperiode.TilDato)

	s := models.Statement{
		SourceRecordID:   rec.ID.String(),
		JournalNumber:    rec.JournalNr,
		FiscalYear:       fiscalYear,
		PeriodStart:      rec.Regnskapsperiode.FraDato,
		PeriodEnd:        rec.Regnskapsperiode.TilDato,
		StatementType:    mapStatementType(rec.Regnskapstype),
		OriginalCurrency: rec.Valuta,
		Metadata: models.StatementMetadata{
			OrganizationForm:    rec.Virksomhet.Organisasjonsform,
			IsParentCompany:     rec.Virksomhet.Morselskap,
			StatementPlan:       rec.Oppstillingsplan,
			AccountingRules:     rec.Regnkapsprinsipper.Regnskapsregler,
			SmallCompany:        rec.Regnkapsprinsipper.SmaaForetak,
			NotAudited:          rec.Revisjon.IkkeRevidertAarsregnskap,
			AuditOptOut:         rec.Revisjon.FravalgRevisjon,
			LiquidationAccounts: rec.Avviklingsregnskap,
		},
		Evidence: models.StatementEvidence{
			Source:         "brreg_regnskapsregisteret",
			SourceURL:      fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s", baseURL, orgNum),
			DetailURL:      fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s/%s", baseURL, orgNum, rec.ID.String()),
			RawPayloadHash: hashStr,
		},
	}

	if r := rec.Resultatregnskap; r != nil {
		if d := r.Driftsresultat; d != nil {
			if d.Driftsinntekter != nil {
				s.RevenueOriginalAmount = numToStr(d.Driftsinntekter.Sum)
				s.SalesRevenueOriginalAmount = numToStr(d.Driftsinntekter.Salgsinntekter)
			}
			s.OperatingProfitOriginalAmount = numToStr(d.Driftsresultat)
		}
		s.ProfitBeforeTaxOriginalAmount = numToStr(r.OrdinaertFoerSkatt)
		s.TaxExpenseOriginalAmount = numToStr(r.Skattekostnad)
		s.NetIncomeOriginalAmount = numToStr(r.Aarsresultat)
		s.TotalResultOriginalAmount = numToStr(r.Totalresultat)

		facts := make(map[string]string)
		if f := r.Finansresultat; f != nil {
			if v := numToStr(f.NettoFinans); v != nil {
				facts["finance_result_original_amount"] = *v
			}
			if f.Finansinntekt != nil {
				if v := numToStr(f.Finansinntekt.Sum); v != nil {
					facts["financial_income_original_amount"] = *v
				}
			}
			if f.Finanskostnad != nil {
				if v := numToStr(f.Finanskostnad.Sum); v != nil {
					facts["financial_cost_original_amount"] = *v
				}
			}
		}
		if len(facts) > 0 {
			s.Facts = facts
		}
	}

	if e := rec.Eiendeler; e != nil {
		s.TotalAssetsOriginalAmount = numToStr(e.Sum)
		if e.Omloep != nil {
			s.CurrentAssetsOriginalAmount = numToStr(e.Omloep.Sum)
		}
		if e.Anlegg != nil {
			s.FixedAssetsOriginalAmount = numToStr(e.Anlegg.Sum)
		}
	}

	if eg := rec.EgKapGjeld; eg != nil {
		if eg.Egenkapital != nil {
			s.TotalEquityOriginalAmount = numToStr(eg.Egenkapital.Sum)
		}
		if g := eg.GjeldOversikt; g != nil {
			s.TotalLiabilitiesOriginalAmount = numToStr(g.Sum)
			if g.Kortsiktig != nil {
				s.ShortTermLiabilitiesOriginalAmount = numToStr(g.Kortsiktig.Sum)
			}
			if g.Langsiktig != nil {
				s.LongTermLiabilitiesOriginalAmount = numToStr(g.Langsiktig.Sum)
			}
		}
	}

	return s
}

// numToStr converts a json.Number to a *string decimal (2dp). Returns nil for empty/zero-value numbers.
func numToStr(n json.Number) *string {
	if n == "" {
		return nil
	}
	d, err := decimal.NewFromString(n.String())
	if err != nil {
		return nil
	}
	s := d.StringFixed(2)
	return &s
}

func mapStatementType(t string) string {
	switch t {
	case "SELSKAP":
		return "company"
	case "KONSERN":
		return "group"
	default:
		return t
	}
}

func extractFiscalYear(tilDato string) int {
	t, err := time.Parse("2006-01-02", tilDato)
	if err != nil {
		return 0
	}
	return t.Year()
}
```

- [ ] **Step 4: Run tests — confirm they pass**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/parser/... -v
```

Expected: all tests PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/internal/parser/
git commit -m "feat: add brreg-financial-service parser package with TDD"
```

---

## Task 5: brregclient package (TDD)

**Files:**
- Modify: `internal/brregclient/client.go`
- Create: `internal/brregclient/client_test.go`

The client makes HTTP calls to data.brreg.no and returns typed errors.

- [ ] **Step 1: Write `internal/brregclient/client_test.go`**

```go
package brregclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../parser/testdata/" + name)
	require.NoError(t, err)
	return b
}

func TestFetchKeyFigures_200(t *testing.T) {
	equinorData := fixture(t, "equinor_list.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/regnskapsregisteret/regnskap/923609016", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write(equinorData)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	raw, err := c.FetchKeyFigures(context.Background(), "923609016")
	require.NoError(t, err)
	require.Equal(t, equinorData, raw)
}

func TestFetchKeyFigures_404_ReturnsErrNotAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "974760673")
	require.ErrorIs(t, err, brregclient.ErrNotAvailable)
}

func TestFetchKeyFigures_500_UnsupportedPlan_BANK(t *testing.T) {
	body := fixture(t, "dnb_500.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(body)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "984851006")

	var planErr *brregclient.UnsupportedPlanError
	require.ErrorAs(t, err, &planErr)
	require.Equal(t, "BANK", planErr.PlanName)
}

func TestFetchKeyFigures_500_UnsupportedPlan_SKADE(t *testing.T) {
	body := fixture(t, "storebrand_500.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(body)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "930553506")

	var planErr *brregclient.UnsupportedPlanError
	require.ErrorAs(t, err, &planErr)
	require.Equal(t, "SKADE", planErr.PlanName)
}

func TestFetchKeyFigures_429_ReturnsRetryableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "000000000")

	var retryErr *brregclient.RetryableError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, 429, retryErr.StatusCode)
}

func TestFetchKeyFigures_503_ReturnsRetryableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "000000000")

	var retryErr *brregclient.RetryableError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, 503, retryErr.StatusCode)
}

func TestFetchPDFYears_200(t *testing.T) {
	yearsData := fixture(t, "equinor_pdf_years.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/regnskapsregisteret/regnskap/aarsregnskap/kopi/923609016/aar", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write(yearsData)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	years, err := c.FetchPDFYears(context.Background(), "923609016")
	require.NoError(t, err)
	require.Len(t, years, 14)
	require.Equal(t, "2011", years[0])
}

func TestFetchPDFYears_404_ReturnsErrNotAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchPDFYears(context.Background(), "974760673")
	require.ErrorIs(t, err, brregclient.ErrNotAvailable)
}
```

- [ ] **Step 2: Run tests — confirm they fail**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/brregclient/...
```

Expected: compilation errors — `brregclient.New`, `brregclient.ErrNotAvailable`, etc. undefined.

- [ ] **Step 3: Write `internal/brregclient/client.go`**

```go
package brregclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/parser"
)

// ErrNotAvailable is returned when BRREG responds 404 for an org number.
var ErrNotAvailable = errors.New("brreg: key figures not available")

// UnsupportedPlanError is returned when BRREG responds 500 with an unsupported statement plan.
type UnsupportedPlanError struct {
	PlanName string
}

func (e *UnsupportedPlanError) Error() string {
	return fmt.Sprintf("brreg: unsupported statement plan %q", e.PlanName)
}

// RetryableError is returned on transient failures (HTTP 429, 5xx, network).
type RetryableError struct {
	StatusCode int
	Msg        string
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("brreg: retryable error (HTTP %d): %s", e.StatusCode, e.Msg)
}

// Config holds client construction parameters.
type Config struct {
	BaseURL        string
	RequestTimeout time.Duration
}

// Client is an HTTP client for the BRREG Regnskapsregister API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a Client. Config.BaseURL defaults to "https://data.brreg.no".
func New(cfg Config) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://data.brreg.no"
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		baseURL: cfg.BaseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// FetchKeyFigures fetches the key-figure list for an org number.
// Returns raw response bytes on HTTP 200.
// Returns ErrNotAvailable on HTTP 404.
// Returns *UnsupportedPlanError on HTTP 500 with known plan error.
// Returns *RetryableError on HTTP 429 or other 5xx.
func (c *Client) FetchKeyFigures(ctx context.Context, orgNum string) ([]byte, error) {
	url := fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s", c.baseURL, orgNum)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build key figures request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &RetryableError{StatusCode: 0, Msg: err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: "read body: " + err.Error()}
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		return body, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotAvailable
	case resp.StatusCode == http.StatusInternalServerError:
		if plan, ok := parser.ParseUnsupportedPlan(body); ok {
			return nil, &UnsupportedPlanError{PlanName: plan}
		}
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: string(body)}
	default:
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: fmt.Sprintf("unexpected status %d", resp.StatusCode)}
	}
}

// FetchPDFYears fetches available annual-account PDF years for an org number.
// Returns ErrNotAvailable on HTTP 404.
func (c *Client) FetchPDFYears(ctx context.Context, orgNum string) ([]string, error) {
	url := fmt.Sprintf("%s/regnskapsregisteret/regnskap/aarsregnskap/kopi/%s/aar", c.baseURL, orgNum)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build pdf years request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &RetryableError{StatusCode: 0, Msg: err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: "read body: " + err.Error()}
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotAvailable
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: fmt.Sprintf("unexpected status %d", resp.StatusCode)}
	}

	return parser.ParsePDFYears(body)
}
```

- [ ] **Step 4: Run tests — confirm they pass**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/brregclient/... -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/internal/brregclient/
git commit -m "feat: add brreg-financial-service brregclient package with TDD"
```

---

## Task 6: service package (TDD)

**Files:**
- Modify: `internal/service/service.go`
- Create: `internal/service/service_test.go`

The service orchestrates brregclient + parser and builds the LookupResponse.

- [ ] **Step 1: Write `internal/service/service_test.go`**

```go
package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/pulsarpoint/brreg-financial-service/internal/service"
	"github.com/stretchr/testify/require"
	"os"
)

type stubClient struct {
	keyFigures map[string][]byte
	keyErrors  map[string]error
	pdfYears   map[string][]string
	pdfErrors  map[string]error
}

func (s *stubClient) FetchKeyFigures(_ context.Context, orgNum string) ([]byte, error) {
	if err, ok := s.keyErrors[orgNum]; ok {
		return nil, err
	}
	return s.keyFigures[orgNum], nil
}

func (s *stubClient) FetchPDFYears(_ context.Context, orgNum string) ([]string, error) {
	if err, ok := s.pdfErrors[orgNum]; ok {
		return nil, err
	}
	if yrs, ok := s.pdfYears[orgNum]; ok {
		return yrs, nil
	}
	return nil, brregclient.ErrNotAvailable
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../parser/testdata/" + name)
	require.NoError(t, err)
	return b
}

func TestLookup_SingleSuccess(t *testing.T) {
	c := &stubClient{
		keyFigures: map[string][]byte{"923609016": fixture(t, "equinor_list.json")},
		pdfYears:   map[string][]string{"923609016": {"2022", "2023", "2024"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records:            []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "923609016"}},
		IncludePDFMetadata: true,
		IncludeRawPayload:  false,
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "brreg-financial-service.lookup.v1", resp.SchemaVersion)
	require.Equal(t, "succeeded", resp.Status)
	require.Equal(t, 1, resp.RecordsSeen)
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 0, resp.RecordsFailed)
	require.Len(t, resp.Results, 1)

	r := resp.Results[0]
	require.Equal(t, "r1", r.RecordID)
	require.Equal(t, "succeeded", r.Status)
	require.Len(t, r.Statements, 1)
	require.NotNil(t, r.PDFMetadata)
	require.Equal(t, []string{"2022", "2023", "2024"}, r.PDFMetadata.AvailableYears)
	require.Contains(t, r.PDFMetadata.DownloadURLTemplate, "923609016")
	require.Empty(t, r.Warnings)

	// raw_payload not included when IncludeRawPayload=false
	require.Nil(t, r.Statements[0].RawPayload)
}

func TestLookup_IncludeRawPayload(t *testing.T) {
	c := &stubClient{
		keyFigures: map[string][]byte{"923609016": fixture(t, "equinor_list.json")},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records:           []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "923609016"}},
		IncludeRawPayload: true,
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.Results[0].Statements[0].RawPayload)
}

func TestLookup_NotAvailable(t *testing.T) {
	c := &stubClient{
		keyErrors: map[string]error{"974760673": brregclient.ErrNotAvailable},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "974760673"}},
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "succeeded", resp.Status) // not_available is a clean outcome
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 0, resp.RecordsFailed)
	r := resp.Results[0]
	require.Equal(t, "not_available", r.Status)
	require.Empty(t, r.Statements)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "financials_not_found", r.Warnings[0].Code)
}

func TestLookup_UnsupportedPlan_WithPDFYears(t *testing.T) {
	c := &stubClient{
		keyErrors: map[string]error{"984851006": &brregclient.UnsupportedPlanError{PlanName: "BANK"}},
		pdfYears:  map[string][]string{"984851006": {"2023", "2024"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records:            []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "984851006"}},
		IncludePDFMetadata: true,
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "succeeded", resp.Status)
	r := resp.Results[0]
	require.Equal(t, "unsupported_statement_plan", r.Status)
	require.Empty(t, r.Statements)
	require.NotNil(t, r.PDFMetadata) // PDF still returned
	require.Equal(t, []string{"2023", "2024"}, r.PDFMetadata.AvailableYears)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "unsupported_statement_plan", r.Warnings[0].Code)
	require.Equal(t, "BANK", r.Warnings[0].Detail["statement_plan"])
}

func TestLookup_ProviderFailure(t *testing.T) {
	c := &stubClient{
		keyErrors: map[string]error{"000000001": &brregclient.RetryableError{StatusCode: 503, Msg: "down"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "000000001"}},
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "failed", resp.Status)
	require.Equal(t, 0, resp.RecordsCompleted)
	require.Equal(t, 1, resp.RecordsFailed)
	r := resp.Results[0]
	require.Equal(t, "failed", r.Status)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "provider_unavailable", r.Warnings[0].Code)
}

func TestLookup_MixedBatch_PartialStatus(t *testing.T) {
	c := &stubClient{
		keyFigures: map[string][]byte{"923609016": fixture(t, "equinor_list.json")},
		keyErrors:  map[string]error{"000000001": &brregclient.RetryableError{StatusCode: 503, Msg: "down"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records: []models.LookupRecord{
			{RecordID: "ok", OrganizationNumber: "923609016"},
			{RecordID: "fail", OrganizationNumber: "000000001"},
		},
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "partial", resp.Status)
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 1, resp.RecordsFailed)
}

func TestLookup_BatchTooLarge(t *testing.T) {
	svc := service.New(&stubClient{}, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 2})
	records := make([]models.LookupRecord, 3)
	for i := range records {
		records[i] = models.LookupRecord{RecordID: "x", OrganizationNumber: "923609016"}
	}
	_, err := svc.Lookup(context.Background(), models.LookupRequest{Records: records})
	require.Error(t, err)
	require.Contains(t, err.Error(), "batch_too_large")
}

func TestLookup_InvalidOrgNumber(t *testing.T) {
	svc := service.New(&stubClient{}, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})
	_, err := svc.Lookup(context.Background(), models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "not-a-number"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_organization_number")
}
```

- [ ] **Step 2: Run tests — confirm they fail**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/service/...
```

Expected: compilation errors — `service.New`, `service.Config` undefined.

- [ ] **Step 3: Write `internal/service/service.go`**

```go
package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/pulsarpoint/brreg-financial-service/internal/parser"
)

var orgNumRegexp = regexp.MustCompile(`^\d{9}$`)

// BRREGClient is the interface the service requires from the HTTP client.
type BRREGClient interface {
	FetchKeyFigures(ctx context.Context, orgNum string) ([]byte, error)
	FetchPDFYears(ctx context.Context, orgNum string) ([]string, error)
}

// Config holds Service construction parameters.
type Config struct {
	BaseURL      string
	MaxBatchSize int
}

// Service orchestrates brregclient and parser to produce LookupResponse values.
type Service struct {
	client BRREGClient
	cfg    Config
}

// New creates a Service.
func New(client BRREGClient, cfg Config) *Service {
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = 1000
	}
	return &Service{client: client, cfg: cfg}
}

// Lookup processes a batch of organization lookups.
// Returns an error only for request-level failures (batch too large, invalid org number).
// Per-record failures are encoded in RecordResult.Status.
func (s *Service) Lookup(ctx context.Context, req models.LookupRequest) (models.LookupResponse, error) {
	if len(req.Records) > s.cfg.MaxBatchSize {
		return models.LookupResponse{}, fmt.Errorf("batch_too_large: max %d, got %d", s.cfg.MaxBatchSize, len(req.Records))
	}

	// Validate org numbers before making any HTTP calls.
	for _, rec := range req.Records {
		norm := normalizeOrgNum(rec.OrganizationNumber)
		if !orgNumRegexp.MatchString(norm) {
			return models.LookupResponse{}, fmt.Errorf("invalid_organization_number: %q", rec.OrganizationNumber)
		}
	}

	start := time.Now()
	results := make([]models.RecordResult, 0, len(req.Records))
	completed, failed := 0, 0

	for _, rec := range req.Records {
		result := s.lookupOne(ctx, rec, req.IncludePDFMetadata, req.IncludeRawPayload)
		results = append(results, result)
		if result.Status == "failed" {
			failed++
		} else {
			completed++
		}
	}

	batchStatus := batchStatus(completed, failed)

	return models.LookupResponse{
		SchemaVersion:    models.SchemaVersion,
		Status:           batchStatus,
		RecordsSeen:      len(req.Records),
		RecordsCompleted: completed,
		RecordsFailed:    failed,
		DurationMs:       time.Since(start).Milliseconds(),
		Results:          results,
	}, nil
}

func (s *Service) lookupOne(ctx context.Context, rec models.LookupRecord, includePDF, includeRaw bool) models.RecordResult {
	orgNum := normalizeOrgNum(rec.OrganizationNumber)
	result := models.RecordResult{
		RecordID:           rec.RecordID,
		OrganizationNumber: orgNum,
		Statements:         []models.Statement{},
		Warnings:           []models.Warning{},
	}

	rawBody, err := s.client.FetchKeyFigures(ctx, orgNum)
	switch {
	case err == nil:
		stmts, parseErr := parser.ParseKeyFigures(rawBody, orgNum, s.cfg.BaseURL)
		if parseErr != nil {
			result.Status = "failed"
			result.Warnings = append(result.Warnings, models.Warning{
				Code:    "parse_error",
				Message: parseErr.Error(),
			})
		} else {
			if !includeRaw {
				for i := range stmts {
					stmts[i].RawPayload = nil
				}
			}
			result.Status = "succeeded"
			result.Statements = stmts
		}

	case errors.Is(err, brregclient.ErrNotAvailable):
		result.Status = "not_available"
		result.Warnings = append(result.Warnings, models.Warning{
			Code:    "financials_not_found",
			Message: "No BRREG annual-account key figures are available",
		})

	default:
		var planErr *brregclient.UnsupportedPlanError
		if errors.As(err, &planErr) {
			result.Status = "unsupported_statement_plan"
			result.Warnings = append(result.Warnings, models.Warning{
				Code:    "unsupported_statement_plan",
				Message: "BRREG key-figure API does not support this statement plan",
				Detail:  map[string]any{"statement_plan": planErr.PlanName},
			})
		} else {
			result.Status = "failed"
			result.Warnings = append(result.Warnings, models.Warning{
				Code:    "provider_unavailable",
				Message: err.Error(),
				Detail:  map[string]any{"category": "external_service", "retry_strategy": "retry_with_backoff"},
			})
		}
	}

	if includePDF {
		result.PDFMetadata = s.fetchPDFMetadata(ctx, orgNum)
	}

	return result
}

func (s *Service) fetchPDFMetadata(ctx context.Context, orgNum string) *models.PDFMetadata {
	years, err := s.client.FetchPDFYears(ctx, orgNum)
	if err != nil {
		if errors.Is(err, brregclient.ErrNotAvailable) {
			return &models.PDFMetadata{AvailableYears: []string{}}
		}
		return &models.PDFMetadata{AvailableYears: []string{}}
	}
	if years == nil {
		years = []string{}
	}
	return &models.PDFMetadata{
		AvailableYears:      years,
		DownloadURLTemplate: fmt.Sprintf("%s/regnskapsregisteret/regnskap/aarsregnskap/kopi/%s/{year}", s.cfg.BaseURL, orgNum),
	}
}

func batchStatus(completed, failed int) string {
	switch {
	case failed == 0:
		return "succeeded"
	case completed == 0:
		return "failed"
	default:
		return "partial"
	}
}

func normalizeOrgNum(s string) string {
	out := make([]byte, 0, 9)
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			out = append(out, s[i])
		}
	}
	return string(out)
}
```

- [ ] **Step 4: Run tests — confirm they pass**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/service/... -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/internal/service/
git commit -m "feat: add brreg-financial-service service package with TDD"
```

---

## Task 7: httpapi package (TDD)

**Files:**
- Modify: `internal/httpapi/handler.go`
- Create: `internal/httpapi/types.go`
- Create: `internal/httpapi/handler_test.go`

- [ ] **Step 1: Write `internal/httpapi/types.go`**

```go
package httpapi

import (
	"context"

	"github.com/pulsarpoint/brreg-financial-service/internal/models"
)

// LookupService is the interface the handler requires from the service layer.
type LookupService interface {
	Lookup(ctx context.Context, req models.LookupRequest) (models.LookupResponse, error)
}
```

- [ ] **Step 2: Write `internal/httpapi/handler_test.go`**

```go
package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/httpapi"
	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/pulsarpoint/brreg-financial-service/internal/service"
	"github.com/stretchr/testify/require"
	"os"
)

// realService builds a service wired to a stub client backed by fixture files.
func realService(t *testing.T) httpapi.LookupService {
	t.Helper()
	equinorData, err := os.ReadFile("../parser/testdata/equinor_list.json")
	require.NoError(t, err)
	pdfYears, err := os.ReadFile("../parser/testdata/equinor_pdf_years.json")
	require.NoError(t, err)
	_ = pdfYears

	return service.New(&handlerStubClient{
		keyFigures: map[string][]byte{"923609016": equinorData},
		keyErrors: map[string]error{
			"974760673": brregclient.ErrNotAvailable,
			"984851006": &brregclient.UnsupportedPlanError{PlanName: "BANK"},
			"000000001": &brregclient.RetryableError{StatusCode: 503, Msg: "down"},
		},
		pdfYears: map[string][]string{
			"923609016": {"2022", "2023", "2024"},
			"984851006": {"2023", "2024"},
		},
	}, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 5})
}

type handlerStubClient struct {
	keyFigures map[string][]byte
	keyErrors  map[string]error
	pdfYears   map[string][]string
}

func (s *handlerStubClient) FetchKeyFigures(_ context.Context, orgNum string) ([]byte, error) {
	if err, ok := s.keyErrors[orgNum]; ok {
		return nil, err
	}
	return s.keyFigures[orgNum], nil
}

func (s *handlerStubClient) FetchPDFYears(_ context.Context, orgNum string) ([]string, error) {
	if yrs, ok := s.pdfYears[orgNum]; ok {
		return yrs, nil
	}
	return nil, brregclient.ErrNotAvailable
}

func TestHealthz(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "ok", body["status"])
}

func TestLookupEndpoint_SingleSuccess(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	reqBody := models.LookupRequest{
		Records:            []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "923609016"}},
		IncludePDFMetadata: true,
		IncludeRawPayload:  false,
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp models.LookupResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "brreg-financial-service.lookup.v1", resp.SchemaVersion)
	require.Equal(t, "succeeded", resp.Status)
	require.Equal(t, 1, resp.RecordsSeen)
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 0, resp.RecordsFailed)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "succeeded", resp.Results[0].Status)
	require.Len(t, resp.Results[0].Statements, 1)
}

func TestLookupEndpoint_MixedBatch(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	reqBody := models.LookupRequest{
		Records: []models.LookupRecord{
			{RecordID: "ok", OrganizationNumber: "923609016"},
			{RecordID: "na", OrganizationNumber: "974760673"},
			{RecordID: "up", OrganizationNumber: "984851006"},
			{RecordID: "fail", OrganizationNumber: "000000001"},
		},
		IncludePDFMetadata: true,
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp models.LookupResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "partial", resp.Status)
	require.Equal(t, 3, resp.RecordsCompleted) // ok, na, up
	require.Equal(t, 1, resp.RecordsFailed)    // fail

	statusMap := make(map[string]string)
	for _, r := range resp.Results {
		statusMap[r.RecordID] = r.Status
	}
	require.Equal(t, "succeeded", statusMap["ok"])
	require.Equal(t, "not_available", statusMap["na"])
	require.Equal(t, "unsupported_statement_plan", statusMap["up"])
	require.Equal(t, "failed", statusMap["fail"])
}

func TestLookupEndpoint_BatchTooLarge(t *testing.T) {
	h := httpapi.NewHandler(realService(t)) // max batch = 5
	records := make([]models.LookupRecord, 6)
	for i := range records {
		records[i] = models.LookupRecord{RecordID: "x", OrganizationNumber: "923609016"}
	}
	b, _ := json.Marshal(models.LookupRequest{Records: records})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLookupEndpoint_InvalidOrgNumber(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	b, _ := json.Marshal(models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "not-9-digits"}},
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLookupEndpoint_EmptyRecords(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	b, _ := json.Marshal(models.LookupRequest{Records: []models.LookupRecord{}})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.True(t, strings.Contains(rec.Body.String(), "records") || rec.Code == http.StatusBadRequest)
}

func TestLookupEndpoint_InvalidJSON(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", strings.NewReader("not json")))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
```

- [ ] **Step 3: Run tests — confirm they fail**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/httpapi/...
```

Expected: compilation error — `httpapi.NewHandler` undefined.

- [ ] **Step 4: Write `internal/httpapi/handler.go`**

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/models"
)

// NewHandler returns an http.Handler with all routes registered.
func NewHandler(svc LookupService) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /v1/brreg/financials/lookup", handleLookup(svc))
	return mux
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleLookup(svc LookupService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		var req models.LookupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		if len(req.Records) == 0 {
			http.Error(w, "records must contain at least 1 entry", http.StatusBadRequest)
			return
		}

		resp, err := svc.Lookup(r.Context(), req)
		if err != nil {
			msg := err.Error()
			if strings.HasPrefix(msg, "batch_too_large") || strings.HasPrefix(msg, "invalid_organization_number") {
				http.Error(w, msg, http.StatusBadRequest)
				return
			}
			slog.Error("lookup request failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		resp.DurationMs = time.Since(start).Milliseconds()
		writeJSON(w, http.StatusOK, resp)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "error", err)
	}
}
```

- [ ] **Step 5: Run tests — confirm they pass**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./internal/httpapi/... -v
```

Expected: all tests PASS.

- [ ] **Step 6: Run full test suite**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go test ./...
```

Expected: all packages pass.

- [ ] **Step 7: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/internal/httpapi/
git commit -m "feat: add brreg-financial-service httpapi package with TDD"
```

---

## Task 8: main.go

**Files:**
- Modify: `cmd/brreg-financial-service/main.go`

- [ ] **Step 1: Write `cmd/brreg-financial-service/main.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/httpapi"
	"github.com/pulsarpoint/brreg-financial-service/internal/service"
)

func main() {
	listenAddr      := getEnv("BRREG_FINANCIAL_SERVICE_LISTEN_ADDR", ":8098")
	baseURL         := getEnv("BRREG_FINANCIAL_SERVICE_BASE_URL", "https://data.brreg.no")
	timeoutStr      := getEnv("BRREG_FINANCIAL_SERVICE_REQUEST_TIMEOUT", "30s")
	maxBatchSizeStr := getEnv("BRREG_FINANCIAL_SERVICE_MAX_BATCH_SIZE", "1000")

	requestTimeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		slog.Error("invalid BRREG_FINANCIAL_SERVICE_REQUEST_TIMEOUT", "value", timeoutStr)
		os.Exit(1)
	}

	var maxBatchSize int
	if _, err := fmt.Sscanf(maxBatchSizeStr, "%d", &maxBatchSize); err != nil {
		slog.Error("invalid BRREG_FINANCIAL_SERVICE_MAX_BATCH_SIZE", "value", maxBatchSizeStr)
		os.Exit(1)
	}

	client := brregclient.New(brregclient.Config{
		BaseURL:        baseURL,
		RequestTimeout: requestTimeout,
	})

	svc := service.New(client, service.Config{
		BaseURL:      baseURL,
		MaxBatchSize: maxBatchSize,
	})

	handler := httpapi.NewHandler(svc)
	server := &http.Server{
		Addr:         listenAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("brreg-financial-service starting", "addr", listenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **Step 2: Build binary**

```bash
cd data-pipelines/services/brreg-financial-service && GOWORK=off go build -o bin/brreg-financial-service ./cmd/brreg-financial-service
```

Expected: `bin/brreg-financial-service` created, no errors.

- [ ] **Step 3: Run `make test` to confirm everything still passes**

```bash
cd data-pipelines/services/brreg-financial-service && make test
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add data-pipelines/services/brreg-financial-service/cmd/
git commit -m "feat: add brreg-financial-service main entrypoint"
```

---

## Task 9: docker-compose entry + smoke test

**Files:**
- Modify: `data-pipelines/services/docker-compose.yml`

- [ ] **Step 1: Add `brreg-financial-service` to `docker-compose.yml`**

Append to `data-pipelines/services/docker-compose.yml` after the existing `currency-service` block:

```yaml
  brreg-financial-service:
    image: ghcr.io/pulsarpoint/corpscout-brreg-financial-service:${SERVICES_IMAGE_TAG:-latest}
    build:
      context: ./brreg-financial-service
      dockerfile: Dockerfile
    env_file:
      - path: ./brreg-financial-service/.env
        required: false
    ports:
      - "${BRREG_FINANCIAL_SERVICE_PORT:-8098}:8098"
    healthcheck:
      test:
        - CMD
        - wget
        - -qO-
        - http://127.0.0.1:8098/healthz
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 5s
    restart: unless-stopped
```

- [ ] **Step 2: Verify docker compose build**

```bash
cd data-pipelines/services && docker compose build brreg-financial-service
```

Expected: image builds successfully, no errors.

- [ ] **Step 3: Start service and smoke test healthz**

```bash
cd data-pipelines/services && docker compose up -d brreg-financial-service
sleep 3
curl -s http://localhost:8098/healthz
```

Expected: `{"status":"ok"}`.

- [ ] **Step 4: Stop service**

```bash
cd data-pipelines/services && docker compose stop brreg-financial-service
```

- [ ] **Step 5: Commit**

```bash
git add data-pipelines/services/docker-compose.yml
git commit -m "feat: add brreg-financial-service to docker-compose at port 8098"
```

---

## Self-review checklist

### Spec coverage

| Requirement | Task |
|---|---|
| POST /v1/brreg/financials/lookup | Task 7 |
| GET /healthz | Task 7 |
| Batch 1–1000 records | Tasks 6, 7 |
| record_id echoed back | Task 6 |
| org number validation (9 digits) | Task 6 |
| not_available on 404 | Tasks 5, 6 |
| unsupported_statement_plan on 500 plan error | Tasks 4, 5, 6 |
| failed with retry_strategy on 429/5xx | Tasks 5, 6 |
| succeeded with null fields for missing data | Tasks 4 (no_revenue fixture) |
| decimal strings via shopspring/decimal | Task 4 |
| json.Decoder.UseNumber() | Task 4 |
| raw_payload_hash = sha256: | Task 4 |
| facts: finance_result, financial_income, financial_cost | Task 4 |
| regnskapstype SELSKAP→company, KONSERN→group | Task 4 |
| evidence.source_url and detail_url | Task 4 |
| pdf_metadata with available_years | Tasks 5, 6 |
| pdf_metadata download_url_template | Task 6 |
| PDF years fetched even for unsupported plan | Task 6 |
| include_raw_payload controls RawPayload field | Task 6 |
| batch status succeeded/partial/failed | Task 6 |
| docker-compose at port 8098 | Task 9 |
| Dockerfile mirrors currency-service | Task 1 |
| GOWORK=off in Makefile | Task 1 |
| All env vars with defaults | Task 8 |
| slog for unexpected errors only | Task 7 |

All spec requirements are covered. No gaps found.
