import psycopg2

from sql_queries import create_table_queries, drop_table_queries
from src.configurations.environment import Configuration


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
