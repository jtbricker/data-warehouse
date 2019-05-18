import logging
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    logger.info("Running drop queries")
    for query in drop_table_queries:
        logger.info("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    logger.info("Running create queries")
    for query in create_table_queries:
        print("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    logger.info('Connecting to db')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
        .format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.info('Connected to db')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    logger.info("Connection closed")

if __name__ == "__main__":
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("./%(filename)s.log"),
        logging.StreamHandler()
    ])
    logger = logging.getLogger()

    logger.info("Starting create_tables.py")
    main()