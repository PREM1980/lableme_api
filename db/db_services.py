'''
Created on Jul 26, 2018

@author: prem
'''

"""PostgresDbContext module"""
import os
import time
import psycopg2.extras
import sys
from log import logger


class PostGresDAO(object):
    """ This module is used to interface with postgres database"""

    @staticmethod
    def build_from_environment():
        """ static utility method that creates a connection object"""

        pg_username = os.environ['POSTGRES_USERNAME']
        pg_password = os.environ['POSTGRES_PASSWORD']
        db_name = os.environ['DBNAME']

        try:
            conn = psycopg2.connect("user=" + pg_username + 
                                    " host='localhost' password=" + pg_password + 
                                    " dbname=" + db_name)
        except psycopg2.OperationalError, oe:
            raise oe
        
        return PostGresDAO(conn)

    def __init__(self, conn, max_retries=3, retry_period=5):
        self.conn = conn
        self._max_retries = max_retries
        self._retry_period = retry_period

    def _get_cursor(self):
        """ returns a cursor for the connection"""
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def execute(self, qry, values):
        """ returns a function that executes the query and returns the result"""
        def internal():
            self._cur = self._get_cursor()
            self._cur.execute(qry, values)
            rows = self._cur.fetchall()
            return rows

        return self._retry(internal)

    def _retry(self, f):
        """ The query is executed multiple times to overcome any operational
            error. Database error is logged immediately."""
        count = 0
        while True:
            try:
                return f()
            # http://initd.org/psycopg/docs/module.html#psycopg2.DatabaseError
            # handle operational error - memory allocation, unexpected disconnect
            except psycopg2.OperationalError, oe:
                count += 1
                if count < self._max_retries:
                    logger.warn("Transient Error Received %s ", oe)
                    time.sleep(self._retry_period)
                else:
                    logger.error("Unrecoverable Error %s", oe)
                    raise oe
            # other database errors - integrity, internal, programming error etc
            except psycopg2.DatabaseError, de:
                logger.error("Database Error %s", de)
                raise de
            # interface errors
            except psycopg2.Error, e:
                raise e

    def close(self):
        """ closes the connection"""
        self._conn.close()
    
class PostGresService(object):
    @staticmethod
    def build_from_environment():
        
        db = PostGresDAO.build_from_environment()
        return PostGresService(db)
    
    def __init__(self, db):
        self.db = db
    
    def execute_qry(self, qry, values):
        try:
            response = self.db.execute(qry, values)
            return response 
        except psycopg2.Error:
            return []
        

if __name__ == '__main__':
    import os
    os.environ['POSTGRES_USERNAME'] = 'postgres'
    os.environ['POSTGRES_PASSWORD'] = 'postgres'
    os.environ['DBNAME'] = 'annotations'
    pg = PostGresService.build_from_environment()
#     print pg.execute_qry("select * from public.customer where customer_id = %(cust_id)s", {'cust_id':1})
    #cursor.execute('SELECT * from table where id = %(some_id)d', {'some_id': 1234})
    
    pg.execute_qry("insert into public.customer (customer_name) values (%(customer_name)s)", {'customer_name':'target'})
    
