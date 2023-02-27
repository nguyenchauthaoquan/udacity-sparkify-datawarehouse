import psycopg2

from sql_queries import copy_table_queries, insert_table_queries
from src.configurations.environment import Configuration


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = Configuration()

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.host,
        config.dbname,
        config.user,
        config.password,
        config.port))

    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
