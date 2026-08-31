"""Canonical financial metric concepts grouped by source.

The outer key is the presentation metric used by source-serving views and the
backoffice. Each source entry is an ordered tuple: earlier concepts win over
later fallbacks when a filing reports more than one candidate.
"""

FINANCIAL_METRIC_MAPPINGS: dict[str, dict[str, tuple[str, ...]]] = {
    "revenue": {
        "bolagsverket": ("Nettoomsattning",),
        "esef": (
            "ifrs-full:Revenue",
            "ifrs-full:RevenueFromContractsWithCustomers",
            "ifrs-full:RevenueAndOperatingIncome",
            "ifrs-full:RevenueFromSaleOfGoods",
            "ifrs-full:RevenueFromRenderingOfServices",
            "ifrs-full:RentalIncomeFromInvestmentProperty",
        ),
    },
    "operating_result": {
        "bolagsverket": ("Rorelseresultat",),
        "esef": (
            "ifrs-full:ProfitLossFromOperatingActivities",
            # Financial institutions (banks, insurers) have no operating-
            # activities subtotal; their reported operating result is profit
            # before tax. Audit 2026-08-31: present in 921 of the 969 parsed
            # filings that the primary concept misses.
            "ifrs-full:ProfitLossBeforeTax",
        ),
    },
    "net_result": {
        "bolagsverket": ("AretsResultat",),
        "esef": ("ifrs-full:ProfitLoss",),
    },
    "total_assets": {
        "bolagsverket": ("Tillgangar", "Balansomslutning"),
        "esef": ("ifrs-full:Assets",),
    },
    "equity": {
        "bolagsverket": ("EgetKapital",),
        "esef": ("ifrs-full:Equity",),
    },
    "liabilities": {
        "esef": ("ifrs-full:Liabilities",),
    },
    "cash_and_bank": {
        "bolagsverket": ("KassaBank", "KassaBankExklRedovisningsmedel"),
        "esef": ("ifrs-full:CashAndCashEquivalents",),
    },
    "current_assets": {
        "bolagsverket": ("Omsattningstillgangar",),
    },
    "current_liabilities": {
        "bolagsverket": ("KortfristigaSkulder",),
    },
    "personnel_expenses": {
        "bolagsverket": ("Personalkostnader",),
    },
    "wages_and_salaries": {
        "bolagsverket": ("LonerAndraErsattningar",),
    },
    "employees": {
        "bolagsverket": ("MedelantaletAnstallda",),
        "esef": ("ifrs-full:AverageNumberOfEmployees",),
    },
}

SOURCE_METRIC_STORAGE_KEYS: dict[str, dict[str, str]] = {
    "bolagsverket": {
        "operating_result": "operating_profit_loss",
        "net_result": "profit_loss",
    },
    "esef": {
        "operating_result": "operating_profit",
        "net_result": "profit_loss",
        "cash_and_bank": "cash",
    },
}


def metric_concepts_for_source(source: str) -> dict[str, tuple[str, ...]]:
    """Return ordered concepts keyed by the source table's metric name."""
    storage_keys = SOURCE_METRIC_STORAGE_KEYS[source]
    return {
        storage_keys.get(canonical_key, canonical_key): source_mappings[source]
        for canonical_key, source_mappings in FINANCIAL_METRIC_MAPPINGS.items()
        if source in source_mappings
    }


def concept_metric_codes_for_source(source: str) -> dict[str, str]:
    """Return source concept -> source table metric code."""
    return {
        concept: metric_code
        for metric_code, concepts in metric_concepts_for_source(source).items()
        for concept in concepts
    }
