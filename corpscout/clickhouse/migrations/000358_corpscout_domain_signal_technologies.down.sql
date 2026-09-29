-- DNS signal retirement (2026-09-29): this migration's object (domain_signal_technologies) was dropped by hand
-- on the server after dns-detect (000468) replaced the old DNS fingerprint pipeline, and its
-- DDL left this file per the dev-phase ledger policy. The file stays for history.
CREATE DATABASE IF NOT EXISTS corpscout;
