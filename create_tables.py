import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print("Running drop queries")
    for query in drop_table_queries:
        print("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    print("Running create queries")
    for query in create_table_queries:
        print("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to db')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to db')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Connection closed")

if __name__ == "__main__":
    print("Starting create_tables.py")
    main()