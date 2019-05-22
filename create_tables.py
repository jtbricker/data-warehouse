import logging
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

""" Run imported scripts to drop and create staging and analytics tables
"""

def drop_tables(cur, conn):
    """Run the imported drop table queries
    
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object
    """
    LOGGER.info("Running drop queries")
    for query in drop_table_queries:
        LOGGER.info("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Run the imported create table queries
    
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object
    """
    LOGGER.info("Running create queries")
    for query in create_table_queries:
        print("Running query: %s" %query)
        cur.execute(query)
        conn.commit()


def main():
    """Setup config, establish connection to db, and call load/insert methods
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    LOGGER.info('Connecting to db')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
        .format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    LOGGER.info('Connected to db')

    drop_tables(cur, conn)
    create_tables(cur, conn)

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

    LOGGER.info("Starting create_tables.py")
    main()