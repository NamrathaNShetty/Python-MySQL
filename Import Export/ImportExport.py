import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class import_export:
    """
        Description: Methods to perform Import and Export of Databas
    """
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
            # self.db_cursor.execute("USE employee_db")
        
        except Exception as e:
            logger.error(e)
    
    def export_db(self):
        '''
        Description:
            This function is used to export database.
        Parameter:
            it takes self as parameter. 
        '''
        try:
            os.system('mysqldump -u root -p employee_db > data-dump.sql')
            logger.info("Export done")

        except Exception as e:
            logger.error()

    def import_db(self):
        '''
        Description:
            This function is used import database.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("CREATE DATABASE import_export")
            logger.info("Database Created")
            os.system('mysql -u root -p import_export < data-dump.sql')
            self.db_cursor.execute("SHOW DATABASES")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
        
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    imp = import_export()
    imp.print_connection()
    imp.export_db()
    imp.import_db()