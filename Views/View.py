import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class views:

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

    def create_view(self):
        '''
        Description:
            This function creates a view from the customers table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("USE employee_db")
            self.db_cursor.execute("CREATE VIEW view AS SELECT id,job_name FROM employee")
            logger.info("View created")
        
        except Exception as e:
            logger.error(e) 

    def display_view(self):
        '''
        Description:
            This function displays the view.
        Parameter:
            it takes self as parameter.
         '''

        try:
            self.db_cursor.execute("SELECT *FROM view")
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)
        
        except Exception as e:
            logger.error(e)   

    def update_view(self):
        '''
        Description:
            This function updates the view.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("ALTER VIEW view AS SELECT id, job_name,salary FROM employee")
            logger.info("View Updated")
        
        except Exception as e:
            logger.error(e) 

    def drop_view(self):
        '''
        Description:
            This function drops a view.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("DROP VIEW view")
            logger.info("View Dropped")

        except Exception as e:
            logger.error(e)                    

if __name__ == "__main__":
    view = views()
    view.print_connection()
    view.create_view()
    view.display_view()
    view.update_view()
    view.drop_view()                   

    