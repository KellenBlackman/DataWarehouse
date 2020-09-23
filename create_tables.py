import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all the tables in drop_table_queries for the created postgres connection.

    Parameters:
        cur(psycopg2 cursor): The cursor to use to drop the tables in drop_tables_queries.
        conn(psycopg2 connection): The connection to the database that holds the Data Warehouse tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all the tables in create_table_queries for the given postgres connection.

    Parameters:
        cur(psycopg2 cursor): The cursor to use to create the tables in create_tables_queries.
        conn(psycopg2 connection): The connection to the database that will hold the Data Warehouse tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """The main function to create_tables.py.

    Reads in the database connection details, creates a database connection and cursor to execute commands.
    Drops all tables in drop_talbes_queries, then creates all tables in create_table_queries.
    Closes the connection to the database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
