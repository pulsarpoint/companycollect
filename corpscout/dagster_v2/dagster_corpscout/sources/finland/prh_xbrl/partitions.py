import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec

# The PRH discovery API is natively windowed by registration date
# (all_financial_statements?registeredDateStart&registeredDateEnd), so the
# registration month is the pull's unit of work, retry, and backfill.
registration_month_partitions = dg.MonthlyPartitionsDefinition(
    start_date=spec.PARTITION_START_DATE,
)
