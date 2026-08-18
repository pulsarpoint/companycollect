import type { FinancialDemoLocale } from "~/components/financial-demo/formatters";

export const financialDemoCopy = {
  en: {
    currentFinancialsPage: "Current financials page",
    prototypeData: "Prototype data",
    annualOverview: "Annual financial overview",
    referencePage: "Reference page",
    language: "Language",
    prototypeTitle: "Standalone design prototype",
    prototypeDescription:
      "Components use static values and do not query ClickHouse. USD uses a fixed illustrative conversion while we review the production data contract and computed financial view.",
    latestFiling: "Latest filing",
    financialYear: "Financial year",
    filingBasis: "12 months · SEK / USD · standalone accounts",
    allSourceFacts: "All source facts",
    sourceFactsForYear: "View source facts for",
    kpis: {
      netTurnover: "Net turnover",
      operatingResult: "Operating result",
      netResult: "Net result",
      equityRatio: "Equity ratio",
      yearOverYear: "YoY",
      margin: "margin",
      points: "pts",
    },
    chart: {
      title: "Five-year performance",
      description:
        "Compact axis values follow SEK / USD order. Hover a point for both exact amounts.",
      financialYear: "Financial year",
      measureLabel: "Financial trend measure",
      revenue: "Revenue",
      operatingResult: "Operating result",
      netResult: "Net result",
      equity: "Equity",
    },
    ratios: {
      title: "Key ratios",
      description:
        "Comparable profitability, liquidity, solvency, and per-employee measures. Monetary values show exact SEK with illustrative USD underneath.",
      rows: {
        quickRatio: "Quick ratio",
        equityRatio: "Equity ratio",
        operatingMargin: "Operating margin",
        ebitda: "EBITDA (prototype)",
        staffCostsPerEmployee: "Staff costs per employee",
        revenuePerEmployee: "Revenue per employee",
        revenueChange: "Revenue change",
      },
    },
    incomeStatement: {
      title: "Income statement · SEK / USD",
      description:
        "A five-year matrix with exact SEK first and illustrative USD second. Totals are emphasized and supporting lines are indented.",
      rows: {
        netTurnover: "Net turnover",
        otherOperatingIncome: "Other operating income",
        totalOperatingIncome: "Total operating income",
        operatingExpenses: "Operating expenses",
        operatingResult: "Operating result",
        resultAfterFinancialItems: "Result after financial items",
        tax: "Tax",
        netResult: "Net result",
      },
    },
    balanceSheet: {
      title: "Balance sheet · SEK / USD",
      description:
        "Assets, equity, and liabilities remain comparable across years, with both currencies visible in every monetary cell.",
      rows: {
        fixedAssets: "Fixed assets",
        currentAssets: "Current assets",
        totalAssets: "Total assets",
        equity: "Equity",
        untaxedReserves: "Untaxed reserves",
        longTermLiabilities: "Long-term liabilities",
        currentLiabilities: "Current liabilities",
        equityAndLiabilities: "Equity and liabilities",
      },
    },
    notes: {
      title: "Source and calculation notes",
      description:
        "The production version should keep every displayed value connected to the filing, exchange rate, and formula that produced it.",
      taggedFacts: "130 tagged facts in 2025",
      officialFiling: "Official filing",
      officialFilingDescription:
        "Bolagsverket digital annual report, period ended 2025-12-31.",
      currencyPair: "SEK / USD for every amount",
      currencyPairDescription:
        "SEK is shown first and illustrative USD underneath. Demo rate: 1 SEK = 0.10 USD.",
      formulaAwareRatios: "Formula-aware ratios",
      formulaAwareRatiosDescription:
        "EBITDA currently equals operating result in this sample because no depreciation or amortization fact is present.",
    },
  },
  sv: {
    currentFinancialsPage: "Nuvarande finansiella sida",
    prototypeData: "Prototypdata",
    annualOverview: "Finansiell årsöversikt",
    referencePage: "Referenssida",
    language: "Språk",
    prototypeTitle: "Fristående designprototyp",
    prototypeDescription:
      "Komponenterna använder statiska värden och frågar inte ClickHouse. USD bygger på en fast illustrativ omräkning medan vi granskar produktionsmodellens datakontrakt och beräknade finansvy.",
    latestFiling: "Senaste årsredovisningen",
    financialYear: "Räkenskapsår",
    filingBasis: "12 månader · SEK / USD · separat redovisning",
    allSourceFacts: "Alla källfakta",
    sourceFactsForYear: "Visa källfakta för",
    kpis: {
      netTurnover: "Nettoomsättning",
      operatingResult: "Rörelseresultat",
      netResult: "Årets resultat",
      equityRatio: "Soliditet",
      yearOverYear: "år/år",
      margin: "marginal",
      points: "procentenheter",
    },
    chart: {
      title: "Femårsutveckling",
      description:
        "Kompakta axelvärden visas i ordningen SEK / USD. Håll pekaren över en punkt för exakta belopp i båda valutorna.",
      financialYear: "Räkenskapsår",
      measureLabel: "Finansiellt mått för utvecklingsgrafen",
      revenue: "Nettoomsättning",
      operatingResult: "Rörelseresultat",
      netResult: "Årets resultat",
      equity: "Eget kapital",
    },
    ratios: {
      title: "Nyckeltal",
      description:
        "Jämförbara mått för lönsamhet, likviditet, soliditet och anställda. Belopp visar exakt SEK med illustrativ USD under.",
      rows: {
        quickRatio: "Kassalikviditet",
        equityRatio: "Soliditet",
        operatingMargin: "Rörelsemarginal",
        ebitda: "EBITDA (prototyp)",
        staffCostsPerEmployee: "Personalkostnad per anställd",
        revenuePerEmployee: "Omsättning per anställd",
        revenueChange: "Omsättningsförändring",
      },
    },
    incomeStatement: {
      title: "Resultaträkning · SEK / USD",
      description:
        "En femårsmatris med exakt SEK först och illustrativ USD därefter. Summor betonas och underliggande rader är indragna.",
      rows: {
        netTurnover: "Nettoomsättning",
        otherOperatingIncome: "Övriga rörelseintäkter",
        totalOperatingIncome: "Summa rörelseintäkter",
        operatingExpenses: "Rörelsekostnader",
        operatingResult: "Rörelseresultat",
        resultAfterFinancialItems: "Resultat efter finansiella poster",
        tax: "Skatt",
        netResult: "Årets resultat",
      },
    },
    balanceSheet: {
      title: "Balansräkning · SEK / USD",
      description:
        "Tillgångar, eget kapital och skulder kan jämföras mellan åren, med båda valutorna synliga i varje beloppscell.",
      rows: {
        fixedAssets: "Anläggningstillgångar",
        currentAssets: "Omsättningstillgångar",
        totalAssets: "Summa tillgångar",
        equity: "Eget kapital",
        untaxedReserves: "Obeskattade reserver",
        longTermLiabilities: "Långfristiga skulder",
        currentLiabilities: "Kortfristiga skulder",
        equityAndLiabilities: "Summa eget kapital och skulder",
      },
    },
    notes: {
      title: "Källor och beräkningsnoter",
      description:
        "Produktionsversionen bör koppla varje visat värde till årsredovisningen, växelkursen och formeln som skapade det.",
      taggedFacts: "130 taggade fakta för 2025",
      officialFiling: "Officiell årsredovisning",
      officialFilingDescription:
        "Bolagsverkets digitala årsredovisning för perioden som slutade 2025-12-31.",
      currencyPair: "SEK / USD för varje belopp",
      currencyPairDescription:
        "SEK visas först och illustrativ USD under. Demokurs: 1 SEK = 0,10 USD.",
      formulaAwareRatios: "Formelbaserade nyckeltal",
      formulaAwareRatiosDescription:
        "EBITDA är för närvarande lika med rörelseresultatet i detta exempel eftersom uppgift om av- och nedskrivningar saknas.",
    },
  },
} as const satisfies Record<FinancialDemoLocale, object>;
