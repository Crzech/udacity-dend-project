import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function loads staging tables using the specified cursor and connection 
    from a S3 bucket with Redshift's COPY command getting queries from sql_queries.py 

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
    print('==== Copying data from S3 using COPY command ====')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done.')


def insert_tables(cur, conn): 
    '''
    This function loads fact and dimension tables using the specified cursor.
    Data comming from staging tables.

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
    print('==== Inserting staging data into fact and dimesion tables ====')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()
    except psycopg2.OperationalError as e:
        print(e)


if __name__ == "__main__":
    main()