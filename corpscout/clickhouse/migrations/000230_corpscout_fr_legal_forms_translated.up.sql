CREATE DATABASE IF NOT EXISTS corpscout;

-- French legal forms in English, beside INSEE's own wording.
--
-- Two providers write here. The well-known forms are curated by hand and
-- inserted as provider='static' -- SARL, SAS and SCI are terms of art whose
-- English wording should not drift. The long tail is machine-translated from
-- INSEE's French label, which is a far safer input than the bare code it
-- replaced.
--
-- So the pick is NOT argMax over version alone. A machine run happening after
-- a curation would otherwise overwrite the reviewed wording, silently and
-- with no way to tell from the view. The tuple makes provider the primary key
-- of the comparison -- static outranks everything -- and version only breaks
-- ties within a provider class. Curation therefore always wins, whenever it
-- was written.
CREATE OR REPLACE VIEW corpscout.fr_legal_forms_translated AS
SELECT
    f.*,
    ifNull(t.translated_text, '') AS label_en
FROM corpscout.fr_legal_forms AS f
LEFT JOIN (
    SELECT source_text_hash,
           argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.fr_legal_forms'
      AND source_column = 'label_fr'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label_fr);

-- Same repair for the four-country entity-type view, which had the same
-- latent ordering. Its labels are entirely curated today, so nothing changes
-- now -- but company_entity_types_translation_load exists and enqueues the
-- same labels to the translator, so a run of it would have taken precedence
-- over the hand-reviewed English purely by being more recent.
CREATE OR REPLACE VIEW corpscout.company_entity_types_translated AS
SELECT
    e.*,
    ifNull(t.translated_text, '') AS source_label_en
FROM corpscout.company_entity_types AS e
LEFT JOIN (
    SELECT source_text_hash,
           argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.company_entity_types'
      AND source_column = 'source_label'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(e.source_label);
