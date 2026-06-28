ALTER TABLE corpscout.br_companies
    MODIFY COLUMN status_date Nullable(Date);

ALTER TABLE corpscout.br_companies
    MODIFY COLUMN activity_start_date Nullable(Date);

ALTER TABLE corpscout.br_establishments
    MODIFY COLUMN status_date Nullable(Date);

ALTER TABLE corpscout.br_establishments
    MODIFY COLUMN activity_start_date Nullable(Date);
