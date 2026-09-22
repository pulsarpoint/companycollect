"""Provision the pilot's application roles and ClickHouse named collection.

Run after migrations with PostgreSQL/ClickHouse administrative credentials in the
process environment. The output is a private credentials file, not console output.
"""

import argparse
import os
import secrets
from contextlib import closing
from pathlib import Path
from urllib.parse import quote, urlsplit

import psycopg2
from clickhouse_driver import Client
from dotenv import dotenv_values, load_dotenv
from psycopg2 import sql

from dagster_v3.defs.common.resources import ObjectStoreResource


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials-file", type=Path, required=True)
    parser.add_argument("--postgres-host-for-clients", required=True)
    parser.add_argument("--s3-endpoint-for-clickhouse")
    args = parser.parse_args()
    load_dotenv(".env")
    admin_url = os.environ["PROCESSING_ADMIN_PG_URL"]
    address = urlsplit(admin_url)
    database = address.path.lstrip("/")
    port = address.port or 5432
    if args.credentials_file.exists():
        credentials = dotenv_values(args.credentials_file)
    else:
        credentials = {
            "PROCESSING_PG_PASSWORD": secrets.token_urlsafe(36),
            "PROCESSING_PG_READER_PASSWORD": secrets.token_urlsafe(36),
            "PROCESSING_CLICKHOUSE_PASSWORD": secrets.token_urlsafe(36),
            "PROCESSING_CLICKHOUSE_USER": "processing_publisher",
        }
        credentials["PROCESSING_PG_URL"] = (
            f"postgresql://processing_worker:{quote(credentials['PROCESSING_PG_PASSWORD'], safe='')}"
            f"@{args.postgres_host_for_clients}:{port}/{database}"
        )
        descriptor = os.open(
            args.credentials_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(descriptor, "w") as output:
            output.write(
                "".join(f"{key}={value}\n" for key, value in credentials.items())
            )
    with closing(psycopg2.connect(admin_url, connect_timeout=10)) as connection:
        with connection, connection.cursor() as cursor:
            cursor.execute("SELECT 'processing.tasks'::regclass")
            for role, password in [
                ("processing_worker", credentials["PROCESSING_PG_PASSWORD"]),
                ("processing_reader", credentials["PROCESSING_PG_READER_PASSWORD"]),
            ]:
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cursor.fetchone() is None:
                    cursor.execute(
                        sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role))
                    )
                cursor.execute(
                    sql.SQL(
                        "ALTER ROLE {} WITH LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 8"
                    ).format(sql.Identifier(role)),
                    (password,),
                )
                cursor.execute(
                    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                        sql.Identifier(database), sql.Identifier(role)
                    )
                )
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA processing TO {}").format(
                        sql.Identifier(role)
                    )
                )
            cursor.execute(
                "GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA processing TO processing_worker"
            )
            cursor.execute(
                "GRANT SELECT ON processing.brave_export TO processing_reader"
            )
            cursor.execute(
                "ALTER ROLE processing_reader SET default_transaction_read_only=on"
            )
            cursor.execute("ALTER ROLE processing_worker SET synchronous_commit=on")
    client = Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.getenv("CLICKHOUSE_SECURE", "").lower() in ("true", "1", "yes"),
    )
    try:
        ObjectStoreResource(bucket="company-brave-history").ensure_bucket()
        exists = client.execute(
            "SELECT name FROM system.named_collections WHERE name='brave_history'"
        )
        command = (
            "ALTER NAMED COLLECTION brave_history SET"
            if exists
            else "CREATE NAMED COLLECTION brave_history AS"
        )
        client.execute(
            command
            + " url=%(url)s NOT OVERRIDABLE, access_key_id=%(access)s NOT OVERRIDABLE, secret_access_key=%(secret)s NOT OVERRIDABLE",
            {
                "url": (
                    args.s3_endpoint_for_clickhouse
                    or os.environ["CORPSCOUT_S3_ENDPOINT"]
                ).rstrip("/")
                + "/company-brave-history/",
                "access": os.environ["CORPSCOUT_S3_ACCESS_KEY"],
                "secret": os.environ["CORPSCOUT_S3_SECRET_KEY"],
            },
            settings={"log_queries": 0},
        )
        exists = client.execute(
            "SELECT name FROM system.named_collections WHERE name='processing_postgres'"
        )
        command = (
            "ALTER NAMED COLLECTION processing_postgres SET"
            if exists
            else "CREATE NAMED COLLECTION processing_postgres AS"
        )
        client.execute(
            command
            + """
            host=%(host)s NOT OVERRIDABLE, port=%(port)s NOT OVERRIDABLE,
            database=%(database)s NOT OVERRIDABLE, user='processing_reader' NOT OVERRIDABLE,
            password=%(password)s NOT OVERRIDABLE
        """,
            {
                "host": args.postgres_host_for_clients,
                "port": port,
                "database": database,
                "password": credentials["PROCESSING_PG_READER_PASSWORD"],
            },
            settings={"log_queries": 0},
        )
        client.execute(
            "CREATE USER IF NOT EXISTS processing_publisher IDENTIFIED WITH sha256_password BY %(password)s",
            {"password": credentials["PROCESSING_CLICKHOUSE_PASSWORD"]},
            settings={"log_queries": 0},
        )
        client.execute(
            "ALTER USER processing_publisher IDENTIFIED WITH sha256_password BY %(password)s",
            {"password": credentials["PROCESSING_CLICKHOUSE_PASSWORD"]},
            settings={"log_queries": 0},
        )
        client.execute(
            "GRANT NAMED COLLECTION ON processing_postgres TO processing_publisher"
        )
        client.execute(
            "GRANT NAMED COLLECTION ON brave_history TO processing_publisher"
        )
        client.execute("GRANT S3 ON *.* TO processing_publisher")
        client.execute("GRANT POSTGRES ON *.* TO processing_publisher")
        client.execute("GRANT CREATE TEMPORARY TABLE ON *.* TO processing_publisher")
        client.execute(
            "GRANT SELECT, INSERT ON corpscout.se_company_brave_search_results_latest_success TO processing_publisher"
        )
        client.execute(
            "GRANT SELECT, INSERT ON corpscout.company_brave_search_results TO processing_publisher"
        )
        client.execute(
            "GRANT SELECT ON corpscout.company_brave_search_results_latest TO processing_publisher"
        )
        client.execute(
            "GRANT SELECT ON corpscout.se_company_brave_search_results_s3_archive TO processing_publisher"
        )
    finally:
        client.disconnect()
    print(
        "Processing storage provisioned; credentials saved to the private output file."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Connection/DDL diagnostics may contain credentials; inspect server logs privately.
        raise SystemExit(
            f"Processing provisioning failed ({type(error).__name__})"
        ) from None
