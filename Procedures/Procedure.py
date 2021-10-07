import os
import mysql.connector
from dotenv import load_dotenv
from Loggers import logger

load_dotenv()

class procedure:

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

    def create_procedures(self):
        '''
        Description:
            This function creates procedure with in,out and inout parameter.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute('''CREATE PROCEDURE SelectAllCustomers()
                                    BEGIN SELECT *FROM customers;
                                    END''')
            logger.info("Procedure created")   

            self.db_cursor.execute('''CREATE PROCEDURE LimitCustomer(IN var1 INT)
                                    BEGIN
                                    SELECT *FROM customers LIMIT var1;
                                    END''')
                                 
            logger.info("Procedure with in parameter created")

            self.db_cursor.execute('''CREATE PROCEDURE CountCity(OUT var1 INT)
                                    BEGIN
                                    SELECT COUNT(City) into var1 FROM customers;
                                    END''')
            logger.info("Procedure with out parameter created")

            self.db_cursor.execute('''CREATE PROCEDURE CountCountry(INOUT var1 VARCHAR(25))
                                    BEGIN
                                    SELECT Country into var1 FROM customers WHERE CustName=var1;
                                    END''')
            logger.info("Procedure with inout parameter created")
        
        except Exception as e:
            logger.error()

   
    def call_procedure(self):
        '''
        Description:
            This function calls already created stored procedure.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("CALL SelectAllCustomers()")
            result = self.db_cursor.fetchall()
           
            for x in result:
                logger.info(x)


        except Exception as e:
            logger.error(e)

    def call_inparameter(self):
        '''
        Description:
            This function calls already created in paramater procedure.
        Parameter:
            it takes self as parameter.
        '''

        try:
            
            self.db_cursor.execute("CALL LimitCustomer(4)")
            result = self.db_cursor.fetchall()

            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)

    def call_outparameter(self):
        '''
        Description:
            This function calls already created out paramater procedure.
        Parameter:
            it takes self as parameter.
        '''

        try:
            
            self.db_cursor.execute("CALL CountCity(@M)") 
            self.db_cursor.execute("SELECT @M)") 

            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)
    
    def call_inoutparameter(self):
        '''
        Description:
            This function calls already created inout paramater procedure.
        Parameter:
            it takes self as parameter.
        '''

        try:
            
            self.db_cursor.execute("SET @M ='Namratha'")
            self.db_cursor.execute("CALL CountCountry(@M)")
            self.db_cursor.execute("SELECT ")
            result = self.db_cursor.fetchall()
            for x in result:
                logger.info(x)

        except Exception as e:
            logger.error(e)

    def drop_procedure(self):
        '''
        Description:
            This function drops a procedure.
        Parameter:
            it takes self as parameter.
        '''

        try:
            self.db_cursor.execute("DROP PROCEDURE SelectAllCustomers")
            self.db_cursor.execute("DROP PROCEDURE LimitCustomer")
            self.db_cursor.execute("DROP PROCEDURE CountCity")
            self.db_cursor.execute("DROP PROCEDURE CountCountry")
            logger.info("Procedure dropped")

        except Exception as e:
            logger.error(e)        

if __name__ == "__main__":
    store = procedure()
    store.print_connection()
    store.create_procedures()
    store.call_procedure()
    store.call_inparameter()
    store.call_outparameter()
    store.call_inoutparameter()
    store.drop_procedure()            