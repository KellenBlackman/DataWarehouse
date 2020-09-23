import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Executes the queries in copy_table_queries give cursor and database connection.

    Parameters:
        cur(psycopg2 cursor): The cursor to execute the queries in copy_table_queries.
        conn(psycopg2 connection): The connection to the database that holds the staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Executes the queries in instert_table_queries to insert data into data warehouse tables.

    Parameters:
        cur(psycopg2 cursor): The cursor to execute the queries.
        conn(psycopg2 connection): The connection to the data warehouse.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """The main function for etl.py.

    Reads in database parameters, then creates a connection and cursor to execute queries.
    Copies S3 buckets into staging tables, then inserts data into data warehouse tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()