# Market turnover: why a plausibility guard exists

`company_market_monthly` and `company_market_summary` publish **traded value** —
`close × volume`, converted to USD. This note records why that figure is
filtered before publication, how the threshold was chosen, and what the guard
deliberately does not attempt.

Written 2026-07-31, from measurements taken the same day.

## The symptom

Brazil's Markets page ranked **AZUL S.A. first at $99.4bn traded in 2025**,
above Petrobras. Azul is a distressed airline that entered restructuring; it is
not the most-traded company in Brazil.

## The cause

`close × volume` is only meaningful when both sides describe the same number of
shares. Around a reverse split they do not:

```
AZUL4   2025-12-22   close    0.81   volume 15,262,900   →   0.01bn BRL
AZUL4   2025-12-23   close 3400.00   volume 44,420,000   → 151.03bn BRL
```

The price multiplies; the volume does not. Four days manufactured roughly
538bn BRL of turnover that never happened.

This is not a single bad record, and not our ingest:

- **Measured across 46 jump events on B3**: median price jump **20×**, median
  volume ratio around it **0.80×**. Volume simply does not move.
- **The jumps are spread thin** — at most three symbols on any one date. A bad
  file or a broken load would hit hundreds at once.
- EODHD carries both `close` and `adjusted_close` and back-adjusts price for
  corporate actions. The `volume` column stays on its original share basis.
  That is a common vendor convention, not a defect unique to this feed.

So the artefact is structural, and `adjusted_close` does not help: volume is
the mismatched side.

Azul is the worst case because `AZUL4` ends at **2025-12-30** — the company left
the feed days after the split, so there is no later data to dilute the inflated
window.

## The guard

Each symbol-day is compared to **that same symbol's median day**, and days above
a fixed multiple are excluded from the published figure.

Per symbol, not per market: a volatile small cap is then judged against itself
rather than against Petrobras. A symbol needs **30 trading days** of history to
be judged at all; below that there is no basis, and it passes through unguarded
rather than being cut on a thin median.

### Choosing the threshold

The multiple is measured, not guessed. Daily turnover as a ratio of the
symbol's own median:

| percentile | ratio |
| --- | --- |
| median day | 1 |
| p99 | 32 |
| p99.9 | 253 |
| p99.99 | 5,579 |
| max | 581,941 |

A genuinely busy day runs to about 32 times normal. What each candidate
threshold would remove, over 2025:

| country | days cut at 50x | days cut at 200x | value cut at 50x | value cut at 200x |
| --- | --- | --- | --- | --- |
| BR | 0.39 | 0.14 | **10.2** | **10.0** |
| SE | 0.66 | 0.15 | 1.9 | **0.3** |
| NO | 0.61 | 0.10 | 2.5 | 1.3 |
| FI | 0.33 | 0.06 | 1.6 | **0.3** |

*(figures are proportions of days and of value, in parts per hundred)*

Read the last two columns together — that is the whole argument:

- **Brazil's inflation is concentrated far out in the tail.** 50x removes 10.2;
  200x removes 10.0. Almost identical, so the lower cut buys nothing there.
- **The 50x–200x band in Sweden and Finland is real trading.** Cutting at 50x
  discards 1.9 and 1.6 of their genuine turnover; at 200x, 0.3.

**200x** therefore removes essentially all of Brazil's bad value while
sacrificing almost none of the Nordics' good value, and it sits just above the
p99.9 of the observed distribution.

### Excluded, never silently

Flagged days are removed from `traded_usd` **and counted** into
`excluded_days` and `excluded_usd` on `company_market_monthly` (migration
000227). The country pages surface the amount set aside.

This is the point of the design. A guard that quietly changes a published
number is the same failure as an export that quietly writes nothing — a shape
this codebase has been bitten by more than once. The figure must be auditable.

Effect on 2025, after the guard:

| country | kept | excluded | excluded days |
| --- | --- | --- | --- |
| BR | $872.2bn | $98.1bn | 277 |
| SE | $642.1bn | $2.2bn | 532 |
| NO | $176.3bn | $2.9bn | 111 |
| FI | $214.9bn | $0.8bn | 67 |

Brazil's top five became Sabesp, Petrobras, Vale, Itaú Unibanco and Banco do
Brasil — recognisable large caps, in place of a distressed airline.

## What this deliberately does not do

**It does not rescale volume to recover the real turnover.** Since the price
ratio is visible, volume could in principle be divided by it. That was
considered and rejected: a price jump alone cannot distinguish a split from a
genuine spike or a vendor typo, and Azul's 4,200x is not a plausible split
ratio at all. Rescaling on a wrong inference **fabricates** turnover, which is
worse than omitting it. Proper split handling needs a real corporate-actions
source, not a number inferred from price — inferring it is precisely the
plausible-but-wrong reasoning that produced the Azul figure to begin with.

**It is a heuristic and will sometimes be wrong.** A genuine 250x day — a
takeover, an index inclusion — is excluded. That is why the threshold sits far
out, and why the amount is recorded rather than deleted.

**Symbols with little history pass through unguarded.** As the EODHD backfill
extends to five years, that population shrinks by itself.

## Implementation notes

The guard lives in `defs/company_markets/sql.py`, in the `pxg` CTE, and applies
to **both** published tables. It was briefly applied to only the monthly totals,
which left the company ranking still showing Azul at $99.4bn while the country
total had already set it aside — two published numbers disagreeing about the
same days. Any change here must touch both.

`eodhd_eod_prices` is never modified. The vendor's data is stored as sent; the
guard is a decision made in the derived facts, which is where a judgement call
belongs.

**Never write a literal per-cent sign in that SQL file, comments included.** The
ClickHouse driver binds parameters by that character, so one inside a comment is
still read as a format spec and the statement dies with `an integer is required,
not dict`. This cost two failed materialisations while the guard was being
written, the second inside the comment explaining the first.
