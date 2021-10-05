import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    This function drops all the tables using the specified cursor and connection
    getting queries from sql_queries.py

    Parameters
    ----------
    cur: psycopg2.cursor
        Cursor used to execute queries

    conn: psycopg2.connection
        Connection used to commit the executed queries

    Raises
    ------
    psycopg2.OperationalError
        Failed to execute a query

    Returns
    -------
    None

    '''
    print('==== Droping existing tables ====')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done.')


def create_tables(cur, conn):
    '''
    This function creates new tables using the specified cursor and connection
    getting queries from sql_queries.py

    Parameters
    ----------
    cur: psycopg2.cursor
        Cursor used to execute queries

    conn: psycopg2.connection
        Connection used to commit the executed queries

    Raises
    ------
    psycopg2.OperationalError
        Failed to execute a query

    Returns
    -------
    None
    '''
    print('==== Creating new tables ====')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        conn.close()
    except psycopg2.OperationalError as e:
        print(e)


if __name__ == "__main__":
    main()