import json

import pytest

from crawler_ratsit.extract import (
    parse_company_page,
    parse_people_at_address,
    validate_ratsit_url,
)

COMPANY_URL = "https://www.ratsit.se/361228NGDB-Lindstrom,_Olof_Gunnar"
PERSON_PATH = "/19361228-Olof_Gunnar_Lindstrom_Skelleftea/person-token"

COMPANY_HTML = f"""
<html>
  <head>
    <script type="application/ld+json">
      {
    json.dumps(
        [
            {
                "@type": "Organization",
                "name": "Lindström, Olof Gunnar",
                "address": {
                    "streetAddress": "Tjärn 22",
                    "postalCode": "93191",
                    "addressLocality": "Skellefteå",
                },
            },
            {
                "@type": "GeoCoordinates",
                "latitude": "64.72557",
                "longitude": "20.97956",
            },
            {"@type": "Article", "dateModified": "2026-08-27"},
            {
                "@type": "ItemList",
                "name": "Arbetsställen",
                "itemListElement": {
                    "@type": "LocalBusiness",
                    "name": "Arbetsställe 49010721",
                    "identifier": "49010721",
                    "description": "01500 - Blandat jordbruk",
                    "address": {
                        "streetAddress": "Tjärn 22",
                        "postalCode": "931 91",
                        "addressLocality": "Skellefteå",
                        "addressRegion": "Västerbottens län",
                    },
                    "numberOfEmployees": {"name": "0 anställda"},
                },
            },
        ],
        ensure_ascii=False,
    )
}
    </script>
  </head>
  <body>
    <main>
      <aside class="quick-facts"><h1>Fallback company name</h1></aside>
      <div id="foretaget">
        <h3>Juridisk Person</h3>
        <div><div>Organisationsnummer:</div><div>361228-XXXX</div></div>
        <div><div>Bolagsform:</div><div>Enskild näringsidkare</div></div>
        <div><div>Status:</div><div><span>Aktiv</span></div></div>
        <h3>Adress</h3>
        <div><div>Gatuadress:</div><div>Fallback street</div></div>
        <h3>Svensk näringsgrensindelning SNI</h3>
        <p><span>01500</span> - Blandat jordbruk</p>
        <p><span>02101</span> - Skogsförvaltning</p>
        <h3>Verksamhetsbeskrivning</h3><p></p>
      </div>
      <h3>Befattningshavare | 1 st</h3>
      <div class="table-standard">
        <table><tbody><tr>
          <td><a href="{PERSON_PATH}">Lindström (89)</a></td>
          <td>Innehavare</td>
        </tr></tbody></table>
      </div>
      <div id="merInfo"></div>
      <div class="row"><h2>Mer om Lindström, Olof Gunnar</h2></div>
      <div class="row"><p>Company narrative.</p></div>
      <h2>Kontaktuppgifter</h2>
      <table>
        <thead><tr><td>Addressuppgifter</td></tr></thead>
        <tbody><tr><td>Län:</td><td>Västerbottens län</td></tr></tbody>
      </table>
      <div id="paAdressen"></div>
      <h3>Personer</h3><p>Inga personer finns registrerade på adressen.</p>
    </main>
  </body>
</html>
"""

PERSON_HTML = """
<html>
  <body>
    <main>
      <div id="paAdressen"><h2>Registrerat på adressen</h2></div>
      <div><address>Tjärn 22<br>931 91 Skellefteå</address></div>
      <div>
        <div>
          <h3>Personer</h3>
          <div class="row mt-10">
            <div><a href="/19970420-Teresia_Lamas_Skelleftea/token-a">
              <strong>Teresia Lämås (29 år)</strong>
            </a></div>
            <div><a href="/kop/kassa/lonekollen/token-a">Kolla lön direkt</a></div>
          </div>
          <div class="row mt-10">
            <div><a href="/19361228-Olof_Gunnar_Lindstrom_Skelleftea/token-b">
              <strong>Olof Gunnar Lindström (89 år)</strong>
            </a></div>
          </div>
        </div>
        <div><h3>Telefonnummer</h3></div>
      </div>
    </main>
  </body>
</html>
"""

FINANCIAL_HTML = """
<html>
  <body>
    <main>
      <div>
        <h2>Översikt senaste koncernbokslut</h2>
        <div class="block-multi-box-four">
          <div class="block-multi-box-four-item">
            <h3><span>Utdelning</span></h3>
            <div class="block-multi-box-four-item__ingress">0 MSEK</div>
          </div>
        </div>
      </div>
      <div>
        <h2>Översikt senaste bolagsbokslut</h2>
        <div class="block-multi-box-four">
          <div class="block-multi-box-four-item">
            <h3><span>Utdelning</span></h3>
            <div class="block-multi-box-four-item__ingress">30&nbsp;480,0 MSEK</div>
          </div>
        </div>
      </div>

      <table>
        <thead><tr><th><span class="text-nowrap">Nyckeltal</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Kassalikviditet (%)</span></td><td>153,1</td></tr>
          <tr><td><span class="text-nowrap">Omsättningsförändring (%)</span></td><td>−0,2</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th><span class="text-nowrap">Bokslutsperiod</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Bokslutsperiod</span></td><td>2025-01-01 2025-12-31</td></tr>
          <tr><td><span class="text-nowrap">Bokslutslängd</span></td><td>12</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th><span class="text-nowrap">Resultaträkning (MSEK)</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Rörelsens omsättning</span></td><td>177&nbsp;034,0</td></tr>
          <tr><td><span class="text-nowrap">Rörelsens kostnader</span></td><td>−170&nbsp;460,0</td></tr>
          <tr><td><span class="text-nowrap">Årets resultat</span></td><td>5&nbsp;772,0</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th><span class="text-nowrap">Balansräkning (MSEK)</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Eget kapital</span></td><td>61&nbsp;760,0</td></tr>
          <tr><td><span class="text-nowrap">Balansomslutning</span></td><td>158&nbsp;292,0</td></tr>
        </tbody>
      </table>

      <table>
        <thead><tr><th><span class="text-nowrap">Nyckeltal</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Kassalikviditet (%)</span></td><td>134,3</td></tr>
          <tr><td><span class="text-nowrap">Omsättningsförändring (%)</span></td><td>2,4</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th><span class="text-nowrap">Bokslutsperiod</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Bokslutsperiod</span></td><td>2025-01-01 2025-12-31</td></tr>
          <tr><td><span class="text-nowrap">Bokslutslängd</span></td><td>12</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th><span class="text-nowrap">Resultaträkning (MSEK)</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Rörelsens omsättning</span></td><td>1&nbsp;398,0</td></tr>
          <tr><td><span class="text-nowrap">Rörelsens kostnader</span></td><td>−1&nbsp;336,0</td></tr>
          <tr><td><span class="text-nowrap">Årets resultat</span></td><td>5&nbsp;131,0</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th><span class="text-nowrap">Balansräkning (MSEK)</span></th><th>2025</th></tr></thead>
        <tbody>
          <tr><td><span class="text-nowrap">Eget kapital</span></td><td>38&nbsp;135,0</td></tr>
          <tr><td><span class="text-nowrap">Balansomslutning</span></td><td>38&nbsp;662,0</td></tr>
        </tbody>
      </table>

      <div id="antal-anstallda">
        <noscript><table><tbody><tr><td>2025</td><td>24880</td></tr></tbody></table></noscript>
      </div>

      <div class="row mt-20">
        <div><h2>Resultat- och balansräkning (MSEK)</h2></div>
        <div class="table-result">
          <h3>Resultaträkning 2025</h3>
          <div class="table-result-content">
            <div class="row"><div>Resultat efter finansnetto</div><div>7&nbsp;268,0</div></div>
          </div>
        </div>
        <div class="table-result">
          <h3>Balansräkning 2025</h3>
          <div class="table-result-content">
            <div class="row"><div>Omsättningstillgångar</div><div>125&nbsp;482,0</div></div>
          </div>
        </div>
        <div class="table-result">
          <h3>Nyckeltal 2025</h3>
          <div class="table-result-content">
            <div class="row"><div>Antal anställda</div><div>24880 st</div></div>
          </div>
        </div>
      </div>
      <div class="row mt-20">
        <div><h2>Resultat- och balansräkning (MSEK)</h2></div>
        <div class="table-result">
          <h3>Resultaträkning 2025</h3>
          <div class="table-result-content">
            <div class="row"><div>Resultat efter finansnetto</div><div>5&nbsp;124,0</div></div>
          </div>
        </div>
        <div class="table-result">
          <h3>Balansräkning 2025</h3>
          <div class="table-result-content">
            <div class="row"><div>Omsättningstillgångar</div><div>435,0</div></div>
          </div>
        </div>
        <div class="table-result">
          <h3>Nyckeltal 2025</h3>
          <div class="table-result-content">
            <div class="row"><div>Antal anställda</div><div>151 st</div></div>
          </div>
        </div>
      </div>
    </main>
  </body>
</html>
"""


def test_parse_company_page_prefers_json_ld_and_uses_xpath_fallbacks() -> None:
    report = parse_company_page(COMPANY_HTML, source_url=COMPANY_URL)

    assert report["company"] == {
        "name": "Lindström, Olof Gunnar",
        "organization_number": "361228-XXXX",
        "legal_form": "Enskild näringsidkare",
        "status": "Aktiv",
        "address": {
            "street": "Tjärn 22",
            "postal_code": "93191",
            "locality": "Skellefteå",
            "county": "Västerbottens län",
        },
        "industry_codes": [
            {"code": "01500", "description": "Blandat jordbruk"},
            {"code": "02101", "description": "Skogsförvaltning"},
        ],
        "business_description": None,
        "summary": ["Company narrative."],
    }
    assert report["responsible_people"] == [
        {
            "display_name": "Lindström (89)",
            "role": "Innehavare",
            "profile_url": f"https://www.ratsit.se{PERSON_PATH}",
        }
    ]
    assert report["workplaces"] == [
        {
            "name": "Arbetsställe 49010721",
            "identifier": "49010721",
            "industry": {"code": "01500", "description": "Blandat jordbruk"},
            "address": {
                "street": "Tjärn 22",
                "postal_code": "931 91",
                "locality": "Skellefteå",
                "county": "Västerbottens län",
            },
            "number_of_employees": "0 anställda",
        }
    ]
    assert report["people_at_address"] == []
    assert report["financials"] == []
    assert report["coordinates"] == {
        "latitude": 64.72557,
        "longitude": 20.97956,
    }
    assert report["source_url"] == COMPANY_URL
    assert report["date_modified"] == "2026-08-27"


def test_parse_company_page_extracts_company_and_consolidated_financials() -> None:
    report = parse_company_page(FINANCIAL_HTML, source_url=COMPANY_URL)

    financials = report["financials"]
    assert isinstance(financials, list)
    assert [financial["scope"] for financial in financials] == [
        "consolidated",
        "company",
    ]

    consolidated = financials[0]
    assert consolidated["monetary_unit"] == "MSEK"
    assert consolidated["periods"] == [
        {
            "fiscal_year": 2025,
            "period_start": "2025-01-01",
            "period_end": "2025-12-31",
            "period_months": 12,
            "income_statement": {
                "revenue": 177034.0,
                "operating_costs": -170460.0,
                "net_income": 5772.0,
                "profit_after_financial_items": 7268.0,
            },
            "balance_sheet": {
                "equity": 61760.0,
                "balance_sheet_total": 158292.0,
                "current_assets": 125482.0,
            },
            "key_ratios": {
                "cash_liquidity_percent": 153.1,
                "revenue_change_percent": -0.2,
            },
            "dividend": 0,
            "employee_count": 24880,
        }
    ]

    company_period = financials[1]["periods"][0]
    assert company_period["income_statement"] == {
        "revenue": 1398.0,
        "operating_costs": -1336.0,
        "net_income": 5131.0,
        "profit_after_financial_items": 5124.0,
    }
    assert company_period["balance_sheet"] == {
        "equity": 38135.0,
        "balance_sheet_total": 38662.0,
        "current_assets": 435.0,
    }
    assert company_period["key_ratios"] == {
        "cash_liquidity_percent": 134.3,
        "revenue_change_percent": 2.4,
    }
    assert company_period["dividend"] == 30480.0
    assert company_period["employee_count"] == 151


def test_parse_people_at_address_ignores_salary_links() -> None:
    people = parse_people_at_address(PERSON_HTML, source_url=COMPANY_URL)

    assert people == [
        {
            "name": "Teresia Lämås",
            "age": 29,
            "profile_url": (
                "https://www.ratsit.se/19970420-Teresia_Lamas_Skelleftea/token-a"
            ),
        },
        {
            "name": "Olof Gunnar Lindström",
            "age": 89,
            "profile_url": (
                "https://www.ratsit.se/19361228-Olof_Gunnar_Lindstrom_Skelleftea/"
                "token-b"
            ),
        },
    ]


@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://www.ratsit.se/company",
        "https://example.com/company",
        "https://user:password@www.ratsit.se/company",
        "https://www.ratsit.se:8443/company",
        "https://www.ratsit.se/",
    ],
)
def test_validate_ratsit_url_rejects_unsafe_or_incomplete_urls(url: str) -> None:
    with pytest.raises(ValueError):
        validate_ratsit_url(url)


def test_validate_ratsit_url_accepts_report_url() -> None:
    assert validate_ratsit_url(COMPANY_URL) == COMPANY_URL
