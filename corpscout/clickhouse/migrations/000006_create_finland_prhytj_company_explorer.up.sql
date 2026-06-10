CREATE OR REPLACE VIEW `corpscout_sources`.`fi_prhytj_company_explorer` AS
WITH
base AS (
  SELECT
    business_id,
    argMax(country_iso2, ingested_at) AS country_iso2,
    argMax(source_slug, ingested_at) AS source_slug,
    argMax(source_run_id, ingested_at) AS source_run_id,
    argMax(source_record_id, ingested_at) AS source_record_id,
    argMax(source_payload_hash, ingested_at) AS source_payload_hash,
    argMax(status, ingested_at) AS status_code,
    argMax(trade_register_status, ingested_at) AS trade_register_status_code,
    nullIf(argMax(registration_date, ingested_at), '') AS registration_date,
    nullIf(argMax(end_date, ingested_at), '') AS end_date,
    argMax(lifecycle_status, ingested_at) AS lifecycle_status,
    argMax(is_active, ingested_at) AS is_active,
    max(ingested_at) AS latest_ingested_at
  FROM `corpscout_sources`.`fi_prhytj_statuses`
  GROUP BY business_id
),
code_labels AS (
  SELECT
    code_list,
    code,
    argMax(description, ingested_at) AS description
  FROM `corpscout_sources`.`fi_prhytj_code_lists`
  WHERE language_code = 'en'
  GROUP BY code_list, code
),
names AS (
  SELECT
    business_id,
    source_run_id,
    nullIf(argMaxIf(name, tuple(ifNull(version, 0), ifNull(source_position, 0), ingested_at), ifNull(is_current, false) AND ifNull(is_primary, false)), '') AS primary_current_name,
    nullIf(argMaxIf(name, tuple(ifNull(version, 0), ifNull(source_position, 0), ingested_at), ifNull(is_current, false)), '') AS current_name,
    nullIf(argMaxIf(name, tuple(ifNull(version, 0), ifNull(source_position, 0), ingested_at), ifNull(is_primary, false)), '') AS latest_primary_name,
    nullIf(argMax(name, tuple(ifNull(version, 0), ifNull(source_position, 0), ingested_at)), '') AS latest_name,
    arraySort(x -> x.1, groupArray(CAST(tuple(
      ifNull(source_position, 0),
      name_type_code,
      name,
      version,
      nullIf(registered_on, ''),
      nullIf(ended_on, ''),
      is_current,
      is_primary
    ) AS Tuple(
      source_position UInt32,
      name_type_code Nullable(String),
      name Nullable(String),
      version Nullable(Int32),
      registered_on Nullable(String),
      ended_on Nullable(String),
      is_current Nullable(Bool),
      is_primary Nullable(Bool)
    )))) AS name_history
  FROM `corpscout_sources`.`fi_prhytj_names`
  GROUP BY business_id, source_run_id
),
business_line_descriptions_en AS (
  SELECT
    business_id,
    source_run_id,
    business_line_item_hash,
    argMax(description, ingested_at) AS description
  FROM `corpscout_sources`.`fi_prhytj_business_line_descriptions`
  WHERE language_code = '3'
  GROUP BY business_id, source_run_id, business_line_item_hash
),
business_lines AS (
  SELECT
    bl.business_id,
    bl.source_run_id,
    nullIf(argMax(bl.business_line_type, tuple(ifNull(bl.is_primary, false), bl.ingested_at)), '') AS main_business_line_code,
    nullIf(argMax(bl.business_line_code_set, tuple(ifNull(bl.is_primary, false), bl.ingested_at)), '') AS main_business_line_code_set,
    nullIf(argMax(d.description, tuple(ifNull(bl.is_primary, false), bl.ingested_at)), '') AS main_business_line_description_en
  FROM `corpscout_sources`.`fi_prhytj_business_lines` AS bl
  LEFT JOIN business_line_descriptions_en AS d
    ON d.business_id = bl.business_id
   AND d.source_run_id = bl.source_run_id
   AND d.business_line_item_hash = bl.source_item_hash
  GROUP BY bl.business_id, bl.source_run_id
),
company_form_descriptions_en AS (
  SELECT
    business_id,
    source_run_id,
    company_form_item_hash,
    argMax(description, ingested_at) AS description
  FROM `corpscout_sources`.`fi_prhytj_company_form_descriptions`
  WHERE language_code = '3'
  GROUP BY business_id, source_run_id, company_form_item_hash
),
company_forms AS (
  SELECT
    cf.business_id,
    cf.source_run_id,
    nullIf(argMax(cf.form_type_code, tuple(ifNull(cf.is_current, false), ifNull(cf.version, 0), cf.ingested_at)), '') AS company_form_code,
    nullIf(argMax(d.description, tuple(ifNull(cf.is_current, false), ifNull(cf.version, 0), cf.ingested_at)), '') AS company_form_description_en,
    arraySort(x -> x.1, groupArray(CAST(tuple(
      ifNull(cf.source_position, 0),
      cf.form_type_code,
      d.description,
      cf.version,
      nullIf(cf.registered_on, ''),
      nullIf(cf.ended_on, ''),
      cf.is_current
    ) AS Tuple(
      source_position UInt32,
      form_type_code Nullable(String),
      description_en Nullable(String),
      version Nullable(Int32),
      registered_on Nullable(String),
      ended_on Nullable(String),
      is_current Nullable(Bool)
    )))) AS company_forms
  FROM `corpscout_sources`.`fi_prhytj_company_forms` AS cf
  LEFT JOIN company_form_descriptions_en AS d
    ON d.business_id = cf.business_id
   AND d.source_run_id = cf.source_run_id
   AND d.company_form_item_hash = cf.source_item_hash
  GROUP BY cf.business_id, cf.source_run_id
),
company_situation_descriptions_en AS (
  SELECT
    business_id,
    source_run_id,
    company_situation_item_hash,
    argMax(description, ingested_at) AS description
  FROM `corpscout_sources`.`fi_prhytj_company_situation_descriptions`
  WHERE language_code = '3'
  GROUP BY business_id, source_run_id, company_situation_item_hash
),
company_situations AS (
  SELECT
    cs.business_id,
    cs.source_run_id,
    nullIf(argMax(cs.situation_type_code, tuple(ifNull(cs.is_current, false), cs.ingested_at)), '') AS company_situation_code,
    nullIf(argMax(d.description, tuple(ifNull(cs.is_current, false), cs.ingested_at)), '') AS company_situation_description_en,
    arraySort(x -> x.1, groupArray(CAST(tuple(
      ifNull(cs.source_position, 0),
      cs.situation_type_code,
      d.description,
      nullIf(cs.registered_on, ''),
      nullIf(cs.ended_on, ''),
      cs.is_current
    ) AS Tuple(
      source_position UInt32,
      situation_type_code Nullable(String),
      description_en Nullable(String),
      registered_on Nullable(String),
      ended_on Nullable(String),
      is_current Nullable(Bool)
    )))) AS company_situations
  FROM `corpscout_sources`.`fi_prhytj_company_situations` AS cs
  LEFT JOIN company_situation_descriptions_en AS d
    ON d.business_id = cs.business_id
   AND d.source_run_id = cs.source_run_id
   AND d.company_situation_item_hash = cs.source_item_hash
  GROUP BY cs.business_id, cs.source_run_id
),
websites AS (
  SELECT
    business_id,
    source_run_id,
    nullIf(argMaxIf(normalized_url, tuple(ifNull(is_primary, false), ingested_at), ifNull(is_current, false) AND ifNull(is_primary, false)), '') AS primary_website,
    nullIf(argMaxIf(normalized_url, ingested_at, ifNull(is_current, false)), '') AS current_website,
    arraySort(x -> (x.1, x.4), groupArray(CAST(tuple(
      nullIf(registered_on, ''),
      url,
      normalized_url,
      host,
      nullIf(ended_on, ''),
      is_current,
      is_primary
    ) AS Tuple(
      registered_on Nullable(String),
      url Nullable(String),
      normalized_url Nullable(String),
      host Nullable(String),
      ended_on Nullable(String),
      is_current Nullable(Bool),
      is_primary Nullable(Bool)
    )))) AS websites
  FROM `corpscout_sources`.`fi_prhytj_websites`
  GROUP BY business_id, source_run_id
),
registered_entry_descriptions_en AS (
  SELECT
    business_id,
    source_run_id,
    registered_entry_item_hash,
    argMax(description, ingested_at) AS description
  FROM `corpscout_sources`.`fi_prhytj_registered_entry_descriptions`
  WHERE language_code = '3'
  GROUP BY business_id, source_run_id, registered_entry_item_hash
),
registered_entries AS (
  SELECT
    re.business_id,
    re.source_run_id,
    arraySort(x -> x.1, groupArray(CAST(tuple(
      ifNull(re.source_position, 0),
      re.register_code,
      register_labels.description,
      re.entry_type_code,
      coalesce(entry_labels.description, red.description),
      re.authority,
      authority_labels.description,
      nullIf(re.registered_on, ''),
      nullIf(re.ended_on, ''),
      re.is_current
    ) AS Tuple(
      source_position UInt32,
      register_code Nullable(String),
      register_name Nullable(String),
      entry_type_code Nullable(String),
      entry_status Nullable(String),
      authority_code Nullable(String),
      authority_name Nullable(String),
      registered_on Nullable(String),
      ended_on Nullable(String),
      is_current Nullable(Bool)
    )))) AS registered_entries
  FROM `corpscout_sources`.`fi_prhytj_registered_entries` AS re
  LEFT JOIN registered_entry_descriptions_en AS red
    ON red.business_id = re.business_id
   AND red.source_run_id = re.source_run_id
   AND red.registered_entry_item_hash = re.source_item_hash
  LEFT JOIN code_labels AS register_labels
    ON register_labels.code_list = 'REK'
   AND register_labels.code = re.register_code
  LEFT JOIN code_labels AS entry_labels
    ON entry_labels.code_list = 'REK_KDI'
   AND entry_labels.code = concat(ifNull(re.register_code, ''), '_', ifNull(re.entry_type_code, ''))
  LEFT JOIN code_labels AS authority_labels
    ON authority_labels.code_list = 'VIRANOM'
   AND authority_labels.code = re.authority
  GROUP BY re.business_id, re.source_run_id
),
post_offices AS (
  SELECT
    business_id,
    source_run_id,
    address_item_hash,
    nullIf(argMaxIf(city, ingested_at, language_code = '3'), '') AS city_en,
    nullIf(argMaxIf(city, ingested_at, language_code = '1'), '') AS city_fi,
    nullIf(argMaxIf(city, ingested_at, language_code = '2'), '') AS city_sv,
    nullIf(argMax(municipality_code, ingested_at), '') AS municipality_code
  FROM `corpscout_sources`.`fi_prhytj_address_post_offices`
  GROUP BY business_id, source_run_id, address_item_hash
),
addresses AS (
  SELECT
    a.business_id,
    a.source_run_id,
    arraySort(x -> x.1, groupArray(CAST(tuple(
      ifNull(a.source_position, 0),
      a.address_type_code,
      a.street,
      a.building_number,
      a.entrance,
      a.apartment_number,
      a.post_office_box,
      a.post_code,
      coalesce(po.city_en, po.city_fi, po.city_sv),
      po.municipality_code,
      a.country,
      nullIf(a.registered_on, '')
    ) AS Tuple(
      source_position UInt32,
      address_type_code Nullable(Int32),
      street Nullable(String),
      building_number Nullable(String),
      entrance Nullable(String),
      apartment_number Nullable(String),
      post_office_box Nullable(String),
      post_code Nullable(String),
      city Nullable(String),
      municipality_code Nullable(String),
      country Nullable(String),
      registered_on Nullable(String)
    )))) AS addresses
  FROM `corpscout_sources`.`fi_prhytj_addresses` AS a
  LEFT JOIN post_offices AS po
    ON po.business_id = a.business_id
   AND po.source_run_id = a.source_run_id
   AND po.address_item_hash = a.source_item_hash
  GROUP BY a.business_id, a.source_run_id
)
SELECT
  b.business_id AS business_id,
  b.country_iso2 AS country_iso2,
  b.source_slug AS source_slug,
  b.source_run_id AS source_run_id,
  b.source_record_id AS source_record_id,
  coalesce(n.primary_current_name, n.current_name, n.latest_primary_name, n.latest_name) AS name,
  b.registration_date AS registration_date,
  b.end_date AS end_date,
  b.status_code AS status_code,
  status_labels.description AS status_description,
  b.trade_register_status_code AS trade_register_status_code,
  trade_labels.description AS trade_register_status_description,
  b.lifecycle_status AS lifecycle_status,
  b.is_active AS is_active,
  bl.main_business_line_code AS main_business_line_code,
  bl.main_business_line_code_set AS main_business_line_code_set,
  bl.main_business_line_description_en AS main_business_line_description_en,
  cf.company_form_code AS company_form_code,
  cf.company_form_description_en AS company_form_description_en,
  cs.company_situation_code AS company_situation_code,
  cs.company_situation_description_en AS company_situation_description_en,
  coalesce(w.primary_website, w.current_website) AS website,
  n.name_history AS name_history,
  re.registered_entries AS registered_entries,
  cf.company_forms AS company_forms,
  cs.company_situations AS company_situations,
  w.websites AS websites,
  a.addresses AS addresses,
  b.source_payload_hash AS source_payload_hash,
  b.latest_ingested_at AS latest_ingested_at
FROM base AS b
LEFT JOIN names AS n
  ON n.business_id = b.business_id
 AND n.source_run_id = b.source_run_id
LEFT JOIN business_lines AS bl
  ON bl.business_id = b.business_id
 AND bl.source_run_id = b.source_run_id
LEFT JOIN company_forms AS cf
  ON cf.business_id = b.business_id
 AND cf.source_run_id = b.source_run_id
LEFT JOIN company_situations AS cs
  ON cs.business_id = b.business_id
 AND cs.source_run_id = b.source_run_id
LEFT JOIN websites AS w
  ON w.business_id = b.business_id
 AND w.source_run_id = b.source_run_id
LEFT JOIN registered_entries AS re
  ON re.business_id = b.business_id
 AND re.source_run_id = b.source_run_id
LEFT JOIN addresses AS a
  ON a.business_id = b.business_id
 AND a.source_run_id = b.source_run_id
LEFT JOIN code_labels AS status_labels
  ON status_labels.code_list = 'STATUS3'
 AND status_labels.code = b.status_code
LEFT JOIN code_labels AS trade_labels
  ON trade_labels.code_list = 'REK_KDI'
 AND trade_labels.code = concat('1_', ifNull(b.trade_register_status_code, ''));
