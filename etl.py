import logging
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

""" Uses sql imported SQL statements to load data into tables
"""


def load_staging_tables(cur, conn):
    """Run the imported copy data queries
    
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object
    """
    LOGGER.info("Running staging queries")
    for query in copy_table_queries:
        LOGGER.info("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Run the imported insert data queries
    
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object
    """
    LOGGER.info("Running insert queries")
    for query in insert_table_queries:
        LOGGER.info("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def main():
    """Setup config, establish connection to db, and call load/insert methods
    """
    LOGGER.info("Starting etl.py")

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    LOGGER.info('Connecting to db')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    LOGGER.info('Connected to db')
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    LOGGER.info("Connection closed")

if __name__ == "__main__":
    """Setup logging
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("./%(filename)s.log"),
            logging.StreamHandler()
    ])
    LOGGER = logging.getLogger()
    
    main()