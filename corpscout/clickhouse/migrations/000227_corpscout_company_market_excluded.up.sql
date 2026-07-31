CREATE DATABASE IF NOT EXISTS corpscout;

-- Record what the turnover plausibility guard removed.
--
-- The guard drops symbol-days whose turnover is wildly out of line with that
-- symbol's own history, because EODHD adjusts price for a reverse split and
-- leaves volume on the pre-split share count -- measured across 46 B3 events,
-- price multiplies by a median 20x while volume moves 0.80x. close x volume
-- then mixes two share bases and inflates turnover by roughly the split ratio.
--
-- Counted rather than silently dropped. A guard that quietly changes a
-- published number is the same disease as an export that quietly writes
-- nothing, and this codebase has been bitten by that shape more than once. The
-- page can now say how much was set aside.
ALTER TABLE corpscout.company_market_monthly
    ADD COLUMN IF NOT EXISTS excluded_days UInt32 DEFAULT 0;

ALTER TABLE corpscout.company_market_monthly
    ADD COLUMN IF NOT EXISTS excluded_usd Decimal(38, 2) DEFAULT 0;
