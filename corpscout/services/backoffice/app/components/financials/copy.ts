export type FinancialLocale = "en" | "sv";

export const financialCopy = {
  en: {
    pageTitle: "Financial overview",
    pageDescription:
      "Five-year development from the selected filed source. Every amount keeps the reported SEK value together with its normalized USD equivalent.",
    language: "Language",
    source: "Bolagsverket annual-report XBRL",
    sourceDescription:
      "Standardized figures from the legal entity's digitally filed annual reports.",
    available: "Financial data available",
    otherFormat: "Filed in another format",
    notSubmitted: "Annual report not submitted",
    unknownStatus: "Filing status unknown",
    latestFiling: "Latest filing",
    financialYear: "Financial year",
    standaloneAccounts: "standalone accounts",
    allSourceFacts: "Latest filing facts",
    sourceFactsForYear: "View source facts for",
    sourceFilings: {
      title: "Source filings",
      description: "Open every tagged fact from each available annual report.",
      factCount: "{count} facts",
      openYear: "Open source facts for {year}",
    },
    reportedIn: "Reported in",
    fromFiling: "from the {year} filing",
    noDataTitle: "No structured financial data available",
    noDataDescription:
      "No digitally filed annual report with standardized financial values is connected to this company yet.",
    unavailableYears: "Filed years without standardized values",
    unavailableYearsDescription:
      "The filings remain available as source evidence, but no displayable financial values were mapped.",
    kpis: {
      revenue: "Net turnover",
      operatingResult: "Operating result",
      netResult: "Net result",
      equityRatio: "Equity ratio",
      yearOverYear: "YoY",
      margin: "margin",
      points: "pts",
      unavailable: "No comparison",
    },
    chart: {
      title: "Five-year performance",
      description:
        "The axis uses reported SEK. Hover a point for the exact SEK and USD values from that filing.",
      financialYear: "Financial year",
      measureLabel: "Financial trend measure",
      revenue: "Net turnover",
      operatingResult: "Operating result",
      netResult: "Net result",
      equity: "Equity",
    },
    ratios: {
      title: "Key ratios",
      description:
        "Calculated from the standardized fields below. A dash means that a required source value was not available.",
      rows: {
        currentRatio: "Current ratio",
        equityRatio: "Equity ratio",
        operatingMargin: "Operating margin",
        staffCostsPerEmployee: "Staff costs per employee",
        revenuePerEmployee: "Revenue per employee",
        revenueChange: "Revenue change",
        employees: "Employees",
      },
    },
    incomeStatement: {
      title: "Income statement · SEK / USD",
      description:
        "Standardized filed income-statement values, with reported SEK first and normalized USD beneath.",
      rows: {
        revenue: "Net turnover",
        personnelExpenses: "Personnel expenses",
        wagesAndSalaries: "Wages and salaries",
        operatingResult: "Operating result",
        netResult: "Net result",
      },
    },
    balanceSheet: {
      title: "Balance sheet · SEK / USD",
      description:
        "Standardized assets, equity, and liabilities for each displayed financial year.",
      rows: {
        cashAndBank: "Cash and bank",
        currentAssets: "Current assets",
        totalAssets: "Total assets",
        equity: "Equity",
        liabilities: "Liabilities",
        currentLiabilities: "Current liabilities",
      },
    },
    notes: {
      title: "Source and calculation notes",
      description:
        "Displayed values remain connected to the filing and conversion metadata used to produce them.",
      taggedFacts: "{count} tagged facts in {year}",
      officialFiling: "Official filing",
      officialFilingDescription:
        "Bolagsverket digital annual report{period} for standalone legal-entity accounts.",
      currencyPair: "SEK / USD for every amount",
      currencyPairDescription:
        "SEK is the filed value. USD uses the stored {source} conversion{date}; it is not an illustrative page-level rate.",
      calculatedRatios: "Calculated ratios",
      calculatedRatiosDescription:
        "Ratios use only the displayed standardized fields. EBITDA is not shown because depreciation and amortization are not available in this view.",
    },
  },
  sv: {
    pageTitle: "Finansiell översikt",
    pageDescription:
      "Femårsutveckling från den valda inlämnade källan. Varje belopp visar rapporterat värde i SEK tillsammans med normaliserat värde i USD.",
    language: "Språk",
    source: "Bolagsverkets årsredovisning i XBRL",
    sourceDescription:
      "Standardiserade värden från den juridiska personens digitalt inlämnade årsredovisningar.",
    available: "Finansiella data tillgängliga",
    otherFormat: "Inlämnad i annat format",
    notSubmitted: "Årsredovisning inte inlämnad",
    unknownStatus: "Inlämningsstatus okänd",
    latestFiling: "Senaste årsredovisningen",
    financialYear: "Räkenskapsår",
    standaloneAccounts: "separat redovisning",
    allSourceFacts: "Fakta från senaste årsredovisningen",
    sourceFactsForYear: "Visa källfakta för",
    sourceFilings: {
      title: "Inlämnade årsredovisningar",
      description:
        "Öppna samtliga taggade fakta från varje tillgänglig årsredovisning.",
      factCount: "{count} fakta",
      openYear: "Öppna källfakta för {year}",
    },
    reportedIn: "Rapporterad i",
    fromFiling: "från årsredovisningen {year}",
    noDataTitle: "Inga strukturerade finansiella data tillgängliga",
    noDataDescription:
      "Ingen digitalt inlämnad årsredovisning med standardiserade finansiella värden är ännu kopplad till företaget.",
    unavailableYears: "Inlämnade år utan standardiserade värden",
    unavailableYearsDescription:
      "Årsredovisningarna finns kvar som källbevis, men inga visningsbara finansiella värden kunde mappas.",
    kpis: {
      revenue: "Nettoomsättning",
      operatingResult: "Rörelseresultat",
      netResult: "Årets resultat",
      equityRatio: "Soliditet",
      yearOverYear: "år/år",
      margin: "marginal",
      points: "procentenheter",
      unavailable: "Ingen jämförelse",
    },
    chart: {
      title: "Femårsutveckling",
      description:
        "Axeln visar rapporterat SEK. Håll pekaren över en punkt för exakta SEK- och USD-värden från årsredovisningen.",
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
        "Beräknade från de standardiserade fälten nedan. Ett streck betyder att ett nödvändigt källvärde saknas.",
      rows: {
        currentRatio: "Balanslikviditet",
        equityRatio: "Soliditet",
        operatingMargin: "Rörelsemarginal",
        staffCostsPerEmployee: "Personalkostnad per anställd",
        revenuePerEmployee: "Omsättning per anställd",
        revenueChange: "Omsättningsförändring",
        employees: "Anställda",
      },
    },
    incomeStatement: {
      title: "Resultaträkning · SEK / USD",
      description:
        "Standardiserade värden från resultaträkningen, med rapporterat SEK först och normaliserat USD under.",
      rows: {
        revenue: "Nettoomsättning",
        personnelExpenses: "Personalkostnader",
        wagesAndSalaries: "Löner och ersättningar",
        operatingResult: "Rörelseresultat",
        netResult: "Årets resultat",
      },
    },
    balanceSheet: {
      title: "Balansräkning · SEK / USD",
      description:
        "Standardiserade tillgångar, eget kapital och skulder för varje visat räkenskapsår.",
      rows: {
        cashAndBank: "Kassa och bank",
        currentAssets: "Omsättningstillgångar",
        totalAssets: "Summa tillgångar",
        equity: "Eget kapital",
        liabilities: "Skulder",
        currentLiabilities: "Kortfristiga skulder",
      },
    },
    notes: {
      title: "Källor och beräkningsnoter",
      description:
        "Visade värden är kopplade till årsredovisningen och den omräkningsmetadata som använts.",
      taggedFacts: "{count} taggade fakta för {year}",
      officialFiling: "Officiell årsredovisning",
      officialFilingDescription:
        "Bolagsverkets digitala årsredovisning{period} för den juridiska personen.",
      currencyPair: "SEK / USD för varje belopp",
      currencyPairDescription:
        "SEK är det inlämnade värdet. USD använder lagrad {source}-omräkning{date}; det är inte en illustrativ sidkurs.",
      calculatedRatios: "Beräknade nyckeltal",
      calculatedRatiosDescription:
        "Nyckeltalen använder endast de visade standardiserade fälten. EBITDA visas inte eftersom av- och nedskrivningar saknas i denna vy.",
    },
  },
} as const;

export const financialSourceCopy = {
  en: {
    selectorTitle: "Financial source",
    selectorDescription:
      "Choose one source and accounting scope. Values are never merged between sources.",
    scopeLabel: "Accounting scope",
    sources: {
      "bolagsverket-annual-accounts": {
        title: "Bolagsverket annual accounts",
        shortTitle: "Bolagsverket",
        scope: "Standalone legal entity",
        description:
          "Standardized figures from the legal entity's digitally filed annual reports.",
        filingDescription:
          "Bolagsverket digital annual report{period} for standalone legal-entity accounts.",
      },
      esef: {
        title: "ESEF consolidated IFRS",
        shortTitle: "ESEF",
        scope: "Consolidated IFRS group",
        description:
          "Standardized group figures from filed ESEF annual financial reports.",
        filingDescription:
          "Filed ESEF annual financial report{period} for consolidated IFRS group accounts.",
      },
    },
  },
  sv: {
    selectorTitle: "Finansiell källa",
    selectorDescription:
      "Välj en källa och redovisningsomfattning. Värden slås aldrig samman mellan källor.",
    scopeLabel: "Redovisningsomfattning",
    sources: {
      "bolagsverket-annual-accounts": {
        title: "Bolagsverkets årsredovisningar",
        shortTitle: "Bolagsverket",
        scope: "Juridisk person",
        description:
          "Standardiserade värden från den juridiska personens digitalt inlämnade årsredovisningar.",
        filingDescription:
          "Bolagsverkets digitala årsredovisning{period} för den juridiska personen.",
      },
      esef: {
        title: "ESEF konsoliderad IFRS",
        shortTitle: "ESEF",
        scope: "Konsoliderad IFRS-koncern",
        description:
          "Standardiserade koncernvärden från inlämnade finansiella ESEF-årsrapporter.",
        filingDescription:
          "Inlämnad finansiell ESEF-årsrapport{period} för en konsoliderad IFRS-koncern.",
      },
    },
  },
} as const;

export type SwedenFinancialSourceId =
  keyof (typeof financialSourceCopy)["en"]["sources"];

export type FinancialCopy = (typeof financialCopy)[FinancialLocale];

export function interpolate(
  template: string,
  values: Record<string, string | number>,
): string {
  return Object.entries(values).reduce(
    (result, [key, value]) => result.replace(`{${key}}`, String(value)),
    template,
  );
}
