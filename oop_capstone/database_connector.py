import mysql.connector as db_connection

class DB_Connector:

    def __init__(self, host: str,user: str, password: str) -> object:
        self.host = host
        self.user = user
        self.password = password
        self.conn = DB_Connector.make_connection(self) 
        self.cursor = self.conn.cursor()
 # ---------------------------------------------------------------------------------------------------------------
    def make_connection(self) -> object:
        # make connection
        conn = db_connection.connect(
            host = self.host,
            user = self.user,
            password = self.password
        )

        return conn
 # ---------------------------------------------------------------------------------------------------------------
    def make_tables(self) -> None:
        # Create database if not exists
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
        # Once it exists, use the db going forward
        self.cursor.execute("USE creditcard_capstone")
        # =========================================================================
        # Create tables if not exists
        # CDW_SAPP_BRANCH
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
                BRANCH_CODE INT PRIMARY KEY,
                BRANCH_NAME VARCHAR(100),
                BRANCH_STREET VARCHAR(100),
                BRANCH_CITY VARCHAR(50),
                BRANCH_STATE VARCHAR(50), 
                BRANCH_ZIP VARCHAR(10),
                BRANCH_PHONE VARCHAR(13),
                LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        # CDW_SAPP_CUSTOMER
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS CDW_SAPP_CUSTOMER (
                    SSN INT(9) PRIMARY KEY,
                    FIRST_NAME VARCHAR(30),
                    MIDDLE_NAME VARCHAR(30),
                    LAST_NAME VARCHAR(30),
                    CREDIT_CARD_NO VARCHAR(19),
                    FULL_STREET_ADDRESS VARCHAR(150),
                    CUST_CITY VARCHAR(30),
                    CUST_STATE VARCHAR(30),
                    CUST_COUNTRY VARCHAR(50),
                    CUST_ZIP VARCHAR(10),
                    CUST_PHONE VARCHAR(13),
                    CUST_EMAIL VARCHAR(60),
                    LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
        # CDW_SAPP_CREDIT_CARD
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD (
                    TRANSACTION_ID INT PRIMARY KEY,
                    CUST_CC_NO VARCHAR(19),
                    TIMEID VARCHAR(8),
                    CUST_SSN INT(9),
                    BRANCH_CODE INT,
                    TRANSACTION_TYPE VARCHAR(50),
                    TRANSACTION_VALUE DOUBLE(10,2),
                    FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN),
                    FOREIGN KEY (BRANCH_CODE) REFERENCES CDW_SAPP_BRANCH(BRANCH_CODE)
                )
            """)
        # CDW_SAPP_LOAN_APPLICATION
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS CDW_SAPP_LOAN_APPLICATION (
                    APPLICATION_ID VARCHAR(30) PRIMARY KEY,
                    APPLICATION_STATUS VARCHAR(2),
                    CREDIT_HISTORY INT,
                    DEPENDENTS CHAR(3),
                    EDUCATION VARCHAR(12),
                    GENDER VARCHAR(20),
                    INCOME VARCHAR(20),
                    MARRIED CHAR(3),
                    PROPERTY_AREA VARCHAR(20),
                    SELF_EMPLOYED VARCHAR(20)
                )
            """)
        # =========================================================================
        # Commit changes
        self.conn.commit()
 # ---------------------------------------------------------------------------------------------------------------
