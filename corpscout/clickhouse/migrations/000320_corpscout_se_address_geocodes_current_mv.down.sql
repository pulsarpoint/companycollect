CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000320: the materialized view goes away and the original serving table comes
-- back under its own name with every row it had. Nothing is lost either way -- the up
-- migration renamed the table rather than dropping it, and the view holds no state of its
-- own beyond a copy of the store read.
--
-- Dropping a materialized view is DROP VIEW, and it takes the view's inner MergeTree table
-- with it. This is the one legitimate drop here: a down file undoing what its own up file
-- created is not a gated drop.

DROP VIEW IF EXISTS corpscout.se_address_geocodes_current;

RENAME TABLE corpscout.se_address_geocodes_current_retired
    TO corpscout.se_address_geocodes_current;
