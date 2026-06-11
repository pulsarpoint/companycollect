\echo verify cluster roles and databases

SELECT
  datname,
  pg_get_userbyid(datdba) AS owner,
  pg_encoding_to_char(encoding) AS encoding,
  datallowconn
FROM pg_database
WHERE datname IN (
  :'corpscout_db',
  :'corpscout_test_db',
  :'temporal_db',
  :'temporal_visibility_db',
  :'dagster_db',
  'postgres'
)
ORDER BY datname;

SELECT
  rolname,
  rolsuper,
  rolinherit,
  rolcreaterole,
  rolcreatedb,
  rolcanlogin,
  rolreplication,
  rolbypassrls
FROM pg_roles
WHERE rolname IN (:'corpscout_user', :'corpscout_anon_role', :'temporal_user', :'dagster_user')
ORDER BY rolname;

SELECT
  member.rolname AS member,
  role.rolname AS role,
  auth.admin_option,
  auth.inherit_option,
  auth.set_option
FROM pg_auth_members auth
JOIN pg_roles role ON role.oid = auth.roleid
JOIN pg_roles member ON member.oid = auth.member
WHERE member.rolname IN (:'corpscout_user', :'temporal_user', :'dagster_user')
   OR role.rolname IN (:'corpscout_anon_role')
ORDER BY member.rolname, role.rolname;

\connect :corpscout_db

\echo verify corpscout database extensions and grants

SELECT
  e.extname,
  e.extversion,
  n.nspname AS schema
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
ORDER BY e.extname;

SELECT
  n.nspname AS schema,
  r.rolname AS grantee,
  has_schema_privilege(r.rolname, n.oid, 'USAGE') AS usage,
  has_schema_privilege(r.rolname, n.oid, 'CREATE') AS create
FROM pg_namespace n
CROSS JOIN pg_roles r
WHERE n.nspname NOT LIKE 'pg_%'
  AND n.nspname <> 'information_schema'
  AND r.rolname IN (:'corpscout_user', :'corpscout_anon_role')
ORDER BY n.nspname, r.rolname;
