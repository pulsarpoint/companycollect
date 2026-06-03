\echo bootstrap databases

SELECT format(
  'CREATE DATABASE %I OWNER %I ENCODING %L TEMPLATE template0',
  :'corpscout_db',
  :'corpscout_user',
  'UTF8'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_database WHERE datname = :'corpscout_db'
)
\gexec

ALTER DATABASE :"corpscout_db" OWNER TO :"corpscout_user";

SELECT format(
  'CREATE DATABASE %I OWNER %I ENCODING %L TEMPLATE template0',
  :'corpscout_test_db',
  :'corpscout_user',
  'UTF8'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_database WHERE datname = :'corpscout_test_db'
)
\gexec

ALTER DATABASE :"corpscout_test_db" OWNER TO :"corpscout_user";

SELECT format(
  'CREATE DATABASE %I OWNER %I ENCODING %L TEMPLATE template0',
  :'temporal_db',
  :'temporal_user',
  'UTF8'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_database WHERE datname = :'temporal_db'
)
\gexec

ALTER DATABASE :"temporal_db" OWNER TO :"temporal_user";

SELECT format(
  'CREATE DATABASE %I OWNER %I ENCODING %L TEMPLATE template0',
  :'temporal_visibility_db',
  :'temporal_user',
  'UTF8'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_database WHERE datname = :'temporal_visibility_db'
)
\gexec

ALTER DATABASE :"temporal_visibility_db" OWNER TO :"temporal_user";
