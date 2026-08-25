CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000320: the materialized view goes away and the original serving table comes
-- back under its own name with every row it had. Nothing is lost either way -- the up
-- migration renamed the table rather than dropping it, and the view holds no state of its
-- own beyond a copy of the store read.
--
-- The up file's staging name does not appear here. Its last statement renamed _next onto
-- the serving name, so after a successful apply no _next exists and there is nothing to
-- undo. A run that failed BEFORE that rename never touched the serving table, and its
-- leftover _next is cleaned up by hand.
--
-- Dropping a materialized view is DROP VIEW, and it takes the view's inner MergeTree table
-- with it. This is the one legitimate drop here: a down file undoing what its own up file
-- created is not a gated drop at all -- its gate is the revert itself.

DROP VIEW IF EXISTS corpscout.se_address_geocodes_current;

RENAME TABLE corpscout.se_address_geocodes_current_retired
    TO corpscout.se_address_geocodes_current;
