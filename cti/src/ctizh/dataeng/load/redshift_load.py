import argparse
import logging
import os

import psycopg


def exec_redshift_query(
    hostname: str,
    port: str,
    database: str,
    username: str,
    password: str,
    sql_query: str,
) -> None:
    # psycopg3 has an error with unicode. see below for details:
    # https://github.com/psycopg/psycopg/issues/122#issuecomment-1281703414
    os.environ["PGCLIENTENCODING"] = "utf-8"

    try:
        conninfo = f"dbname={database} host={hostname} port={port} user={username} password={password}"
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                conn.commit()
    except Exception as e:
        logging.error(f"Error loading data from S3 to Redshift: {str(e)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Parquet data")

    parser.add_argument("--hostname", type=str, help="Hostname", required=True)
    parser.add_argument("--port", type=str, help="Port", required=True)
    parser.add_argument("--database", type=str, help="Database", required=True)
    parser.add_argument("--username", type=str, help="Username", required=True)
    parser.add_argument("--password", type=str, help="Password", required=True)
    parser.add_argument("--sql-query", type=str, help="SQL Query", required=True)

    args = parser.parse_args()

    exec_redshift_query(
        args.hostname,
        args.port,
        args.database,
        args.username,
        args.password,
        args.sql_query,
    )


if __name__ == "__main__":
    main()
