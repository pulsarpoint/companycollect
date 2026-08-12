CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.commoncrawl_domain_ip_connections_ingest_mv;
DROP VIEW IF EXISTS corpscout.commoncrawl_ip_network_segments_ingest_mv;
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_ip_connections;
DROP TABLE IF EXISTS corpscout.commoncrawl_ip_network_segments;
