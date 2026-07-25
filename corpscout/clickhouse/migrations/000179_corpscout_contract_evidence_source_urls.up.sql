CREATE DATABASE IF NOT EXISTS corpscout;

-- Trace each contract back to the document it came from. source_references
-- already carried the source's own id, but an id is not a link: reaching the
-- original notice meant knowing each portal's URL scheme by hand.
--
-- Array, matching source_slugs and source_references, because one canonical
-- evidence row can merge two sources describing the same contract and each
-- keeps its own document.
--
-- Empty for sources that publish no per-award URL. Sweden's UHM is one: it
-- names the advertising database (Mercell, e-Avrop, KommersAnnons) but no
-- per-notice address, so an empty array there is honest rather than missing.
ALTER TABLE corpscout.company_government_contract_evidence
    ADD COLUMN IF NOT EXISTS source_urls Array(String) AFTER source_references;
