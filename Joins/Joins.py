import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class joins:

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

    def display(self):
        '''
        Description:
            This function Display the data of the table.
        Parameter:
            it takes self as parameter.
        '''
        try:
            self.db_cursor.execute("USE joins_db")
            self.db_cursor.execute("SELECT *FROM employee")

            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute("SELECT *FROM customer")

            result2 = self.db_cursor.fetchall()

            for x1 in result2:
                logger.info(x1)
        
        except Exception as e:
            logger.error(e)

    def innerjoin(self):
        '''
        Description:
            This function performs INNER JOIN.
        Parameter:
            it takes self as paramter.
        '''
        
        try:
            self.db_cursor.execute('''SELECT employee.EMP_NAME, customer.CUST_NAME FROM employee 
                                    INNER JOIN customer ON employee.ID = customer.ID''')
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)   

    def left_join(self):
        '''
        Description:
            This function performs LEFT JOIN.
        Parameter:
            it takes self as paramter.
        '''
        
        try:
            self.db_cursor.execute('''SELECT employee.EMP_NAME, customer.CUST_NAME FROM employee 
                                    LEFT JOIN customer ON employee.ID = customer.ID''')
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute('''SELECT employee.JOB_NAME, customer.SALARY FROM employee 
                                    LEFT JOIN customer ON employee.ID = customer.ID''')
            result = self.db_cursor.fetchall()

            for x1 in result:
                logger.info(x1)    

        except Exception as e:
            logger.error(e)    

    def right_join(self):
        '''
        Description:
            This function performs RIGHT JOIN.
        Parameter:
            it takes self as paramter.
        '''
        
        try:
            self.db_cursor.execute('''SELECT employee.EMP_NAME, customer.CUST_NAME FROM employee 
                                    RIGHT JOIN customer ON employee.ID = customer.ID''')
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute('''SELECT employee.JOB_NAME, customer.SALARY FROM employee 
                                    RIGHT JOIN customer ON employee.ID = customer.ID''')
            result1 = self.db_cursor.fetchall()

            for x1 in result1:
                logger.info(x1)
        
        except Exception as e:
            logger.error(e)  


    def full_join(self):
        '''
        Description:
            This function performs RIGHT JOIN.
        Parameter:
            it takes self as paramter.
        '''
        
        try:
            self.db_cursor.execute('''SELECT employee.EMP_NAME, customer.CUST_NAME FROM employee 
                                    LEFT JOIN customer ON employee.ID = customer.ID
                                    UNION SELECT employee.EMP_NAME, customer.CUST_NAME FROM employee 
                                    RIGHT JOIN customer ON employee.ID = customer.ID''')
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)          
            
    def cross_join(self):
        '''
        Description:
            This function performs CROSS JOIN.
        Parameter:
            it takes self as paramter.
        '''
        
        try:
            self.db_cursor.execute('''SELECT employee.EMP_NAME, customer.CUST_NAME FROM employee 
                                    CROSS JOIN customer ''')
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute('''SELECT employee.JOB_NAME, customer.SALARY FROM employee 
                                    CROSS JOIN customer ''')
            result1 = self.db_cursor.fetchall()

            for x1 in result1:
                logger.info(x1)
        
        except Exception as e:
            logger.error(e)

    def self_join(self):
        '''
        Description:
            This function performs SELF JOIN.
        Parameter:
            it takes self as paramter.
        '''
        
        try:
            self.db_cursor.execute('''SELECT A.ID AS ID1, B.ID AS ID2, A.job_name
                                      FROM employee A, employee B
                                      WHERE A.emp_name = B.emp_name''')
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

            self.db_cursor.execute('''SELECT A.id,B.emp_name
                                      FROM employee AS A, employee B
                                      WHERE A.job_name = B.job_name''')
            result = self.db_cursor.fetchall()

            for x1 in result:
                logger.info(x1)
    

        except Exception as e:
            logger.error(e)        
                     

if __name__ == "__main__":
    join = joins()
    join.print_connection()
    join.display()
    join.innerjoin()
    join.left_join()
    join.right_join()
    join.full_join()
    join.cross_join()
    join.self_join()
   
    