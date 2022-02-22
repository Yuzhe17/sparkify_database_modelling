from sql_queries import create_table_queries,drop_table_queries
import psycopg2

def create_database():
    '''creating the sparkify database

    returns:
        cur: a cursor object
        conn: a connection to a PostgreSQL database instance
    '''

    #connect to default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=9764")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    #create sparkify database
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    

    # connect to sparkify database
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=9764")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur,conn):
    '''drop all tables
    args:
        cur: a cursor object
        conn: a connection to a PostgreSQL database instance        
    '''

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    '''create all tables
    args:
        cur: a cursor object
        conn: a connection to a PostgreSQL database instance        
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    '''create relevant database and tables
    '''

    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()