from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, substring, lpad, col, initcap, lower, trim, concat_ws
import mysql.connector as dbconnection
# ---------------------------------------------------------------------------------------------------------------
# NOTE: remember that you need to use a raw string because of the windows configuration
spark = SparkSession.builder \
            .appName('capstone-prac') \
            .config("spark.jars", r"C:\Spark\jars\mysql-connector-j-8.0.33.jar") \
            .getOrCreate()
# ---------------------------------------------------------------------------------------------------------------
def extract():
    
    # gather the nessesary data from their data sources and assign them variable names.
    df_branch =spark.read.option("multiLine", True).json('../origin_data/cdw_sapp_branch.json')
    df_customer =spark.read.option("multiLine", True).json('../origin_data/cdw_sapp_customer.json')
    df_credit =spark.read.option("multiLine", True).json('../origin_data/cdw_sapp_credit.json')

    return df_branch, df_customer, df_credit
# ---------------------------------------------------------------------------------------------------------------
def transform(df_branch, df_customer, df_credit) -> tuple:

    # Fill missing 'BRANCH_ZIP' with 99999
    df_branch = df_branch.fillna({'BRANCH_ZIP': 99999})

    df_branch = df_branch.withColumn(
        # select the column I want to change 
        'BRANCH_PHONE',
        # use the concat function with the literal and substring functions to manipulate the data to what you want it to be similar to splicing
        concat(
            lit("("), substring("BRANCH_PHONE", 1, 3), lit(")"),
            substring("BRANCH_PHONE", 4, 3), lit("-"),
            substring("BRANCH_PHONE", 7, 4) 
        )
    )
    # for testing purposes, make sure this is commented out for production
    # df_branch.show(10)

    # =========================================================================
    # rename the <old_column_name> with the <new_column_name>
    df_credit = df_credit.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

    df_credit = df_credit.withColumn(
        "TIMEID",
        concat(
            lpad(col("YEAR").cast("string"), 4, "0"),
            lpad(col("MONTH").cast("string"), 2, "0"),
            lpad(col("DAY").cast("string"), 2, "0")
        ).cast("int")
    )
    df_credit = df_credit.drop("YEAR", "MONTH", "DAY")
    # for testing purposes, make sure this is commented out for production
    # df_credit.show(10)
    # =========================================================================
    # Capitalize first name (only first letter upper)
    df_customer = df_customer.withColumn("FIRST_NAME", initcap(trim(col("FIRST_NAME"))))

    # Lowercase middle name
    df_customer = df_customer.withColumn("MIDDLE_NAME", lower(trim(col("MIDDLE_NAME"))))

    # Capitalize last name (only first letter upper)
    df_customer = df_customer.withColumn("LAST_NAME", initcap(trim(col("LAST_NAME"))))

    # Combine street and apartment into full address (as string)
    df_customer = df_customer.withColumn(
        "FULL_STREET_ADDRESS",
        concat_ws(", ",
            trim(col("STREET_NAME")),
            trim(col("APT_NO").cast("string"))
        )
    )
    df_customer = df_customer.drop("STREET_NAME","APT_NO")
    # Format CUST_PHONE as (XXX)XXX-XXXX, only if it's exactly 10 digits
    df_customer = df_customer.withColumn(
        "CUST_PHONE",
        concat(
            lit("("),
            substring(lpad(col("CUST_PHONE").cast("string"), 10, "X"), 1, 3),
            lit(")"),
            substring(lpad(col("CUST_PHONE").cast("string"), 10, "X"), 4, 3),
            lit("-"),
            substring(lpad(col("CUST_PHONE").cast("string"), 10, "X"), 7, 4)
        )
    )
    # for testing purposes, make sure this is commented out for production
    # df_customer.show(10)
    return df_branch, df_customer, df_credit
# ---------------------------------------------------------------------------------------------------------------
def make_db_and_tables():
    # make connection
    conn = dbconnection.connect(
        host='localhost',
        user='root',
        password='password'
    )
    # make the cursor object
    cursor = conn.cursor()
    
    # Create database if not exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
    # Once it exists, use the db going forward
    cursor.execute("USE creditcard_capstone")
    # =========================================================================
    # Create tables if not exists
    # CDW_SAPP_BRANCH
    cursor.execute("""
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
    cursor.execute("""
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
    cursor.execute("""
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
    # =========================================================================
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
# ---------------------------------------------------------------------------------------------------------------
def load(df_branch, df_customer, df_credit) -> tuple:
    # MySQL JDBC connection properties
    url = "jdbc:mysql://localhost:3306/creditcard_capstone"  # Update with your DB details
    properties = {
        "user": "root",   # Replace with your MySQL username
        "password": "password",  # Replace with your MySQL password
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    # Write the data to MySQL 
    # CDW_SAPP_BRANCH
    df_branch.write.jdbc(url=url, table="CDW_SAPP_BRANCH", mode="append", properties=properties)
    # CDW_SAPP_CUSTOMER
    df_customer.write.jdbc(url=url, table="CDW_SAPP_CUSTOMER", mode="append", properties=properties)
    # CDW_SAPP_CREDIT_CARD
    df_credit.write.jdbc(url=url, table="CDW_SAPP_CREDIT_CARD", mode="append", properties=properties)
# ---------------------------------------------------------------------------------------------------------------
def main():
    print("Starting the ETL process")
    # extract the data needed (pyspark)
    df_branch, df_customer, df_credit = extract()
    print("Extraction complete")
    print() # for spacing
    # transform (pyspark)
    df_branch, df_customer, df_credit = transform(df_branch, df_customer, df_credit)
    print("Transformation complete")
    print() # for spacing
    # create the db and the tables (mysql.connector)
    make_db_and_tables()
    print("Database and tables created")
    print() # for spacing
    # load into the db (pyspark)
    load(df_branch, df_customer, df_credit)
    print("Loading complete")
    print() # for spacing
    print("Finished the ETL process")
# ---------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()