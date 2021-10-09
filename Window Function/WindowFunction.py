import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class window_function:

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
            self.db_cursor.execute("USE customer_db")
        
        except Exception as e:
            logger.error(e)

    def partition_by(self):
        '''
        Description:
            This function implemented group by for window functions.
        Parameter:
            it takes self as parametr.
        '''

        try:
            self.db_cursor.execute("USE employee_db")
            self.db_cursor.execute('''SELECT id, emp_name, job_name ,SUM(Salary) 
                                    OVER ( PARTITION BY job_name) 
                                    AS Total_salary FROM employee;''')
                                    
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)

            self.db_cursor.execute('''SELECT id, emp_name, job_name,salary,COUNT(job_name)
                                    OVER ( PARTITION BY salary) 
                                    AS Count_job FROM employee;''')
                                    
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)    
        
        except Exception as e:
            logger.error(e)

    def analytical_function(self):
        '''
        Description:
            This function implemented group by for analytical functions.
        Parameter:
            it takes self as parametr.
        '''

        try:
            self.db_cursor.execute('''SELECT id, emp_name,job_name, salary,   
                                    NTile(4) OVER() AS salary   
                                    FROM employee;''')
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
        
            self.db_cursor.execute('''SELECT id, emp_name, job_name,LEAD(salary,1) 
                                    OVER(ORDER BY id) AS salary   
                                    FROM employee;''')
            result1 = self.db_cursor.fetchall()
            for x in result1:
                logger.info(x)

        except Exception as e:
            logger.error(e)

    def ranking_function(self):
        '''
        Description:
            This function implemented group by for ranking functions.
        Parameter:
            it takes self as parametr.
        '''

        try:
            self.db_cursor.execute('''SELECT id,emp_name, job_name,   
                                    RANK() OVER() AS salary_rank  
                                    FROM employee;''')
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
        
            self.db_cursor.execute('''SELECT id, emp_name, job_name, DENSE_RANK() 
                                    OVER(ORDER BY id) AS salary_dense_rank   
                                    FROM employee;''')
            result1 = self.db_cursor.fetchall()
            for x in result1:
                logger.info(x)

        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    window = window_function()
    window.print_connection()
    window.partition_by()
    window.analytical_function()
    window.ranking_function()
    