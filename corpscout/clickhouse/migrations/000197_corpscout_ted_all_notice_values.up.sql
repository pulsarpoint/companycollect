CREATE DATABASE IF NOT EXISTS corpscout;

-- TED publishes ten distinct monetary elements per award notice. We stored two,
-- and not the common ones: BT-720, the per-winner amount the product calls "the
-- contract value", appears on 21 of 100 sampled SE/FI/NO notices, while BT-27's
-- estimated contract amount -- discarded -- appears on 57. For most TED notices
-- the money sat in the S3 snapshot and reached no column.
--
-- The grains are kept apart rather than flattened. An estimate, a framework
-- ceiling and a realized award are different claims about different things, and
-- a single column holding whichever happened to be present makes all three
-- unreadable. A framework ceiling summed as spend overstates it wildly.
--
-- Nothing here is coalesced. The view and the UI decide which figure to show,
-- where the choice can be labelled with the business term behind it.

-- --------------------------------------------------------------------------
-- Notice grain: the procedure-wide figures.
-- --------------------------------------------------------------------------
ALTER TABLE corpscout.ted_notices
    -- BT-27: estimated value of the whole procedure. The most commonly
    -- published amount in the register, and previously dropped entirely.
    ADD COLUMN IF NOT EXISTS estimated_value_amount_original Nullable(Decimal(38, 2)) AFTER total_value_currency,
    ADD COLUMN IF NOT EXISTS estimated_value_amount_usd Nullable(Decimal(38, 2)) AFTER estimated_value_amount_original,
    ADD COLUMN IF NOT EXISTS estimated_value_currency LowCardinality(String) AFTER estimated_value_amount_usd,
    -- BT-709: the ceiling a framework agreement may reach. NOT spend.
    ADD COLUMN IF NOT EXISTS framework_maximum_amount_original Nullable(Decimal(38, 2)) AFTER estimated_value_currency,
    ADD COLUMN IF NOT EXISTS framework_maximum_amount_usd Nullable(Decimal(38, 2)) AFTER framework_maximum_amount_original,
    ADD COLUMN IF NOT EXISTS framework_maximum_currency LowCardinality(String) AFTER framework_maximum_amount_usd,
    -- BT-118: maximum value of all contracts under the framework. NOT spend.
    ADD COLUMN IF NOT EXISTS framework_total_maximum_amount_original Nullable(Decimal(38, 2)) AFTER framework_maximum_currency,
    ADD COLUMN IF NOT EXISTS framework_total_maximum_amount_usd Nullable(Decimal(38, 2)) AFTER framework_total_maximum_amount_original,
    ADD COLUMN IF NOT EXISTS framework_total_maximum_currency LowCardinality(String) AFTER framework_total_maximum_amount_usd,
    -- BT-1118: approximate value of all contracts under the framework.
    ADD COLUMN IF NOT EXISTS framework_total_approximate_amount_original Nullable(Decimal(38, 2)) AFTER framework_total_maximum_currency,
    ADD COLUMN IF NOT EXISTS framework_total_approximate_amount_usd Nullable(Decimal(38, 2)) AFTER framework_total_approximate_amount_original,
    ADD COLUMN IF NOT EXISTS framework_total_approximate_currency LowCardinality(String) AFTER framework_total_approximate_amount_usd;

-- --------------------------------------------------------------------------
-- Winner grain: what this winner was paid, and what it subcontracts away.
-- --------------------------------------------------------------------------
ALTER TABLE corpscout.ted_notice_winners
    -- BT-553: the share of the tender to be subcontracted.
    ADD COLUMN IF NOT EXISTS subcontracting_amount_original Nullable(Decimal(38, 2)) AFTER awarded_currency,
    ADD COLUMN IF NOT EXISTS subcontracting_amount_usd Nullable(Decimal(38, 2)) AFTER subcontracting_amount_original,
    ADD COLUMN IF NOT EXISTS subcontracting_currency LowCardinality(String) AFTER subcontracting_amount_usd;

-- --------------------------------------------------------------------------
-- Lot grain: new table.
--
-- A notice's lots carry their own estimates and their own award outcomes, and
-- there was nowhere to put either. Project-side and result-side figures share a
-- row because efac:LotResult is 1:1 with cac:ProcurementProjectLot -- verified
-- over 75 notices, 116 lots, 116 results, no duplicates, no dangling refs.
--
-- ORDER BY carries no Nullable column (allow_nullable_key is off).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corpscout.ted_notice_lots
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    publication_number String,
    lot_id String,
    lot_title String,
    -- BT-27 at lot grain.
    estimated_value_amount_original Nullable(Decimal(38, 2)),
    estimated_value_amount_usd Nullable(Decimal(38, 2)),
    estimated_value_currency LowCardinality(String),
    -- BT-709 at lot grain.
    framework_maximum_amount_original Nullable(Decimal(38, 2)),
    framework_maximum_amount_usd Nullable(Decimal(38, 2)),
    framework_maximum_currency LowCardinality(String),
    -- BT-271: framework ceiling as stated on the award.
    framework_value_maximum_amount_original Nullable(Decimal(38, 2)),
    framework_value_maximum_amount_usd Nullable(Decimal(38, 2)),
    framework_value_maximum_currency LowCardinality(String),
    -- BT-660: revised framework estimate.
    framework_value_reestimated_amount_original Nullable(Decimal(38, 2)),
    framework_value_reestimated_amount_usd Nullable(Decimal(38, 2)),
    framework_value_reestimated_currency LowCardinality(String),
    -- BT-710 / BT-711: the range of admissible tenders received. Neither is an
    -- award. Together they say how competitive the lot was.
    lower_tender_amount_original Nullable(Decimal(38, 2)),
    lower_tender_amount_usd Nullable(Decimal(38, 2)),
    lower_tender_currency LowCardinality(String),
    higher_tender_amount_original Nullable(Decimal(38, 2)),
    higher_tender_amount_usd Nullable(Decimal(38, 2)),
    higher_tender_currency LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    publication_date Nullable(Date),
    partition_key LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (country_iso2, publication_number, lot_id);
