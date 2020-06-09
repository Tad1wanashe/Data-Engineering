import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
Load_staging_tables function.
This function loads the staging tables in amazon redshift.
"""

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
insert_tables function.
This function inserts data into our tables.
"""

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
"""
Main function.
This function connects us to the database and reads information from the dwh.cfg file.
"""

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()