\echo bootstrap dagster database

SELECT format(
  'CREATE ROLE %I LOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'dagster_user',
  :'dagster_password'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_roles WHERE rolname = :'dagster_user'
)
\gexec

ALTER ROLE :"dagster_user"
  WITH LOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'dagster_password';

SELECT format(
  'CREATE DATABASE %I OWNER %I ENCODING %L TEMPLATE template0',
  :'dagster_db',
  :'dagster_user',
  'UTF8'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_database WHERE datname = :'dagster_db'
)
\gexec

ALTER DATABASE :"dagster_db" OWNER TO :"dagster_user";

REVOKE ALL ON DATABASE :"dagster_db" FROM PUBLIC;

GRANT CONNECT, TEMPORARY, CREATE
  ON DATABASE :"dagster_db"
  TO :"dagster_user";
