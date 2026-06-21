from datetime import date
from decimal import Decimal

from dagster_v3.defs.xbrl_common.parser import parse_ixbrl

# Synthetic iXBRL: current + prior instant contexts, a duration (P&L) context, and a
# dimensioned current context. Exercises period selection, scale, sign, dimensions.
SAMPLE = """<?xml version="1.0"?>
<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:core="http://xbrl.frc.org.uk/fr/2025-01-01/core"
      xmlns:dim="http://example.com/dim">
<body>
  <xbrli:context id="cur"><xbrli:entity>
    <xbrli:identifier scheme="http://www.companieshouse.gov.uk/">01234567</xbrli:identifier>
    </xbrli:entity><xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period></xbrli:context>
  <xbrli:context id="prior"><xbrli:entity>
    <xbrli:identifier scheme="http://www.companieshouse.gov.uk/">01234567</xbrli:identifier>
    </xbrli:entity><xbrli:period><xbrli:instant>2024-12-31</xbrli:instant></xbrli:period></xbrli:context>
  <xbrli:context id="cur_dur"><xbrli:entity>
    <xbrli:identifier scheme="http://www.companieshouse.gov.uk/">01234567</xbrli:identifier>
    </xbrli:entity><xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate>
    <xbrli:endDate>2025-12-31</xbrli:endDate></xbrli:period></xbrli:context>
  <xbrli:context id="cur_dim"><xbrli:entity>
    <xbrli:identifier scheme="http://www.companieshouse.gov.uk/">01234567</xbrli:identifier>
    </xbrli:entity><xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period>
    <xbrli:scenario><xbrldi:explicitMember dimension="dim:D">dim:M</xbrldi:explicitMember>
    </xbrli:scenario></xbrli:context>

  <ix:nonFraction name="core:NetAssetsLiabilities" contextRef="cur" unitRef="GBP">1,495,313</ix:nonFraction>
  <ix:nonFraction name="core:NetAssetsLiabilities" contextRef="prior" unitRef="GBP">1,200,000</ix:nonFraction>
  <ix:nonFraction name="core:NetAssetsLiabilities" contextRef="cur_dim" unitRef="GBP">999</ix:nonFraction>
  <ix:nonFraction name="core:TurnoverRevenue" contextRef="cur_dur" unitRef="GBP" scale="3">1,234</ix:nonFraction>
  <ix:nonFraction name="core:ProfitLoss" contextRef="cur_dur" unitRef="GBP" sign="-">5,000</ix:nonFraction>
</body></html>
"""


def test_parses_entity_and_period():
    doc = parse_ixbrl(SAMPLE)
    assert doc.entity_id == "01234567"
    assert doc.reporting_period_end == date(2025, 12, 31)


def test_metric_picks_current_undimensioned():
    doc = parse_ixbrl(SAMPLE)
    # current period, undimensioned (not the 1.2M prior, not the 999 dimensioned).
    assert doc.metric({"NetAssetsLiabilities"}) == Decimal("1495313")


def test_metric_applies_scale():
    doc = parse_ixbrl(SAMPLE)
    # 1,234 with scale=3 -> 1,234,000.
    assert doc.metric({"TurnoverRevenue", "Turnover"}) == Decimal("1234000")


def test_metric_applies_sign():
    doc = parse_ixbrl(SAMPLE)
    assert doc.metric({"ProfitLoss"}) == Decimal("-5000")


def test_missing_concept_is_none():
    doc = parse_ixbrl(SAMPLE)
    assert doc.metric({"DoesNotExist"}) is None


def test_namespace_resolved():
    doc = parse_ixbrl(SAMPLE)
    net = [f for f in doc.facts if f.concept == "NetAssetsLiabilities"][0]
    assert net.namespace == "http://xbrl.frc.org.uk/fr/2025-01-01/core"
