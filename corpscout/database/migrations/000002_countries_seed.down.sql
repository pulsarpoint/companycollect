-- database/migrations/000002_countries_seed.down.sql
DELETE FROM countries WHERE iso_alpha2 IN (
    'GB','NO','EE','DK','US','DE','FR','NL','SE','FI','CH','AT','BE','IE',
    'PL','ES','IT','PT','AU','NZ','CA','SG','JP','IN','BR'
);
