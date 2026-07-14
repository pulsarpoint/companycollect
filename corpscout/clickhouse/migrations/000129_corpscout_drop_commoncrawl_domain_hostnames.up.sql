CREATE DATABASE IF NOT EXISTS corpscout;

-- Both DNS scanners now persist hostname evidence only in the shared DNS observation history.
-- corpscout.domain_hostnames projects the normalized A, AAAA, and CNAME owner inventory from that
-- history, so the former writable registry is redundant and must not remain as a competing source.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_hostnames;
