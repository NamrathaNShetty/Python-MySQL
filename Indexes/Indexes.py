import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class indexes:

    def __init__(self):
     
        host=os.getenv('HOST')
        user=os.getenv('USER1')
        passwd=os.getenv('PASSWD')
        auth_plugin=os.getenv('AUTH_PLUGIN')
        self.db_connection = mysql.connector.connect(
            host= host,
            user=user,
            passwd=passwd,
            auth_plugin=auth_plugin
        )
        self.db_cursor = self.db_connection.cursor()

    def print_connection(self):
        '''
        Description:
            this function prints the connection object.
        Parameter:
            it takes self as parameter.
        '''
        
        try:
            logger.info(self.db_connection)
        
        except Exception as e:
            logger.error(e)

    def create_index(self):
        '''
        Description:
            This function creates a index.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("USE employee_db")
            #self.db_cursor.execute("CREATE INDEX salary ON employee(ID, EMP_NAME, SALARY)")
            self.db_cursor.execute("CREATE UNIQUE INDEX index_name ON employee(ID)")
            logger.info("Index created")

        except Exception as e:
            logger.error(e)

    def display_index(self):
        '''
        Description:
            This function shows the index on table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SHOW INDEX FROM employee")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)    

    def select(self):
        '''
        Description:
            This function select and dispalys records salary greater than 20000 by index.
        Parameter:
            it takes self as parameter. 
        '''

        try:
            self.db_cursor.execute("EXPLAIN SELECT * FROM employee WHERE SALARY >= 20000")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
        
        except Exception as e:
            logger.error(e)

    def drop_index(self):
        '''
        Description:
            This function drops the index from table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("ALTER TABLE employee DROP INDEX index_name")
            logger.info("Index Dropped")

        except Exception as e:
            logger.error(e)       

if __name__ == "__main__":
    index = indexes()
    index.print_connection()
    index.create_index()
    index.display_index()
    index.select()
    index.drop_index()

