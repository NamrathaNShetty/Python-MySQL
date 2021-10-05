import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class crud_operation:
    """
        Description: Methods to perform CRUD operation.
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
        
        except Exception as e:
            logger.error(e)

    def create_db(self):
        '''
        Description:
            This function creates database, display, and drop database.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("CREATE DATABASE employee_db")
            self.db_cursor.execute("CREATE DATABASE customer_db")
            self.db_cursor.execute("SHOW DATABASES")

            for db in self.db_cursor:
                logger.info(db)
                #print(db)

            self.db_cursor.execute("DROP DATABASE customer_db")
            self.db_cursor.execute("SHOW DATABASES")

            for db in self.db_cursor:
                logger.info(db)
                #print(db)
        
        except Exception as e:
            logger.error(e)

    def create_table(self):
        '''
        Description:
            This function creates a table and display table.
        Parameter:
            it takes self as parameter.
        '''
        
        try:
            self.db_cursor.execute("USE employee_db")

            self.db_cursor.execute("CREATE TABLE employee (id INT, emp_name VARCHAR(20),job_name VARCHAR(20),salary INT(6))")

            self.db_cursor.execute("SHOW TABLES")

            for table in self.db_cursor:
                logger.info(table)
	            #print(table)

        except Exception as e:
            logger.error(e)
    
    def alter(self):
        '''
        Description:
            This function adds the constraint primary key to Id.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("USE employee_db")

            self.db_cursor.execute("ALTER TABLE employee MODIFY id INT PRIMARY KEY")

        except Exception as e:
            logger.error(e)


    def insert(self):
        '''
        Description:
            This function used to insert values into  employee table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("USE employee_db")

            employee_sql_query = "INSERT INTO employee(id,emp_name,job_name,salary) VALUES(1, 'Namratha','Manager',15600)"

            self.db_cursor.execute(employee_sql_query)

            self.db_connection.commit()

            logger.info(self.db_cursor.rowcount)
            logger.info("Record Inserted")
            #print("Record Inserted")
        
        except Exception as e:
            logger.error(e)

    def insert_many(self):
        '''
        Description:
            This function used to insert values into employee table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            sql = "INSERT INTO employee(id, emp_name,job_name,salary) VALUES(%s, %s,%s,%s)"
            val = [(2, 'Dinitha','ASE',12500),
                    (3, 'Lohith','Clerk',20900),
                    (4, 'Shubham','TL',89000),
                    (5, 'Shobhith','SSE',54250)]

            self.db_cursor.executemany(sql, val)

            self.db_connection.commit()

            logger.info(self.db_cursor.rowcount)
            #print(self.db_cursor.rowcount)
            logger.info("Record Inserted")
            #print("Record Inserted")
        
        except Exception as e:
            logger.error(e)

    def select(self):
        '''
        Description:
            This function Display the data of the table.
        Parameter:
            it takes self as parameter.
        '''
        try:
            self.db_cursor.execute("SELECT *FROM employee")

            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)
                #print(x)

        except Exception as e:
            logger.error(e)

    def where(self):
        '''
        Description:
            This function checks where conditions and returns the row.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SELECT *FROM employee WHERE ID = 2")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)


        except Exception as e:
            logger.error(e)

    def having(self):
        '''
        Description:
            This function checks having conditions and returns the row.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SELECT emp_name,SUM(salary) FROM employee GROUP BY emp_name HAVING SUM(salary) > 20900")
            result = self.db_cursor.fetchall()
            for x1 in result:
                logger.info(x1)
                #print(x)


        except Exception as e:
            logger.error(e)        
    
    def update(self):
        '''
        Description:
            This function updates the row in a table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("UPDATE employee SET EMP_NAME = 'Mamatha' WHERE ID = 1")
            self.db_connection.commit()
            logger.info(self.db_cursor.rowcount)
            #print(self.db_cursor.rowcount)
            logger.info("Record Inserted")
            #print("Record Inserted")
        
        except Exception as e:
            logger.error(e)

    def delete(self):
        '''
        Description:
            This function delete a row in a table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("DELETE FROM employee WHERE ID = 5")
            self.db_connection.commit()
            logger.info(self.db_cursor.rowcount)
            #print(self.db_cursor.rowcount)
            logger.info("Record Deleted")
            #print("Record Deleted")
        
        except Exception as e:
            logger.error(e)
    
    def orderby(self):
        '''
        Description:
            This function sorts the row.
        Parameter:
            it takes self as parameter. 
        '''

        try:
            self.db_cursor.execute("SELECT *FROM employee ORDER BY EMP_NAME")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)
            
            self.db_cursor.execute("SELECT *FROM employee ORDER BY SALARY DESC")
            result1 = self.db_cursor.fetchall()
            for x1 in result1:
                logger.info(x1)
                #print(x)
        
        except Exception as e:
            logger.error(e)

    def groupby(self):
        '''
        Description:
            This function arrange identical data into groups.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SELECT EMP_NAME, SUM(SALARY) FROM employee GROUP BY EMP_NAME")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)
            
        except Exception as e:
            logger.error(e)
    
    def limit(self):
        '''
        Description: 
            This function display the rows by using limit.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SELECT *FROM employee LIMIT 2")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)
            
        except Exception as e:
            logger.error(e)
    
    def distinct(self):
        '''
        Description:
            This function deletes duplicate records from table.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SELECT DISTINCT EMP_NAME FROM employee")
            result = self.db_cursor.fetchall()
            for x in result:
               logger.info(x)
               #print(x)
            
        except Exception as e:
            logger.error(e)

    def aggregate_functions(self):
        '''
        Description:
            This function arrange identical data into groups and give condition by using having clause .
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("SELECT SUM(SALARY) FROM employee")
            result = self.db_cursor.fetchall()
            #logger.info(result)
            print(result[0][0])
            
            self.db_cursor.execute("SELECT MAX(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)
            #print(result1)

            self.db_cursor.execute("SELECT MIN(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)
            #print(result1)

            self.db_cursor.execute("SELECT AVG(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)
            #print(result1)

            self.db_cursor.execute("SELECT COUNT(SALARY) FROM employee")
            result1 = self.db_cursor.fetchall()
            logger.info(result1)
            #print(result1)

        except Exception as e:
            logger.error(e)
    
    def like(self):
        '''
        Description:
            This function uses for pattern matching by using like operator.
        Parameter:
            it takes self as parameter.
        '''
        try:
            self.db_cursor.execute("SELECT *FROM employee WHERE EMP_NAME LIKE 'M%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)

            self.db_cursor.execute("SELECT *FROM employee WHERE EMP_NAME LIKE 'S%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)
            
            self.db_cursor.execute("SELECT *FROM employee WHERE EMP_NAME LIKE '_a%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)
            
            self.db_cursor.execute("SELECT *FROM employee WHERE SALARY LIKE '2_%_%'")
            result = self.db_cursor.fetchall()
            for x in result:
               logger.info(x)
               #print(x)

            self.db_cursor.execute("SELECT *FROM employee WHERE SALARY LIKE '___00%'")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)
                #print(x)
                
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    crud = crud_operation()
    crud.print_connection()
    crud.create_db()
    crud.create_table()
    crud.alter()
    crud.insert()
    crud.insert_many()
    crud.select()
    crud.where()
    crud.having()
    crud.update()
    crud.delete()
    crud.orderby()
    crud.groupby()
    crud.limit()
    crud.distinct()
    crud.aggregate_functions()
    crud.like()