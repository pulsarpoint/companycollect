\echo bootstrap database grants

GRANT CONNECT, TEMPORARY, CREATE
  ON DATABASE :"corpscout_db"
  TO :"corpscout_user";

REVOKE ALL ON DATABASE :"temporal_db" FROM PUBLIC;
REVOKE ALL ON DATABASE :"temporal_visibility_db" FROM PUBLIC;

GRANT CONNECT, TEMPORARY, CREATE
  ON DATABASE :"temporal_db"
  TO :"temporal_user";

GRANT CONNECT, TEMPORARY, CREATE
  ON DATABASE :"temporal_visibility_db"
  TO :"temporal_user";
