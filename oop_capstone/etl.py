import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, substring, lpad, col, initcap, lower, trim, concat_ws, when, length

class Etl_Pipeline:

    def __init__(self) -> object:
        """
            Constructor function:
                This is the Etl_Pipeline class' constructor. The SparkSession is made and stored in the self.spark variable
                and the dataframes that will be used are declared with empty strings for visual purposes. The intent is all 
                the variables that will be used throughout the class in multiple functions will displayed in the __init__ method.   
        """
        # NOTE: remember that you need to use a raw string because of the windows configuration
        self.spark = SparkSession.builder \
                .appName('capstone-proj') \
                .config("spark.jars", r"C:\Spark\jars\mysql-connector-j-8.0.33.jar") \
                .getOrCreate()
        # these will be assign values in the extract portion. This is here to provide a schema to refer to for the class.
        self.df_loan = "" 
        self.df_branch = "" 
        self.df_customer = "" 
        self.df_credit = ""
 # ---------------------------------------------------------------------------------------------------------------
    def extract(self) -> None:
        """
            Parameters explained: 
                None 

            Return values:
                None

            Explanation of function: 
                This function will extract the data and store the in spark dataframes.
        """ 
        # from the api
        res = requests.get("https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json") 
        json_data = res.json()
        # gather the necessary data from their data sources and assign them variable names.
        self.df_loan = self.spark.createDataFrame(json_data)
        self.df_branch = self.spark.read.option("multiLine", True).json('../origin_data/cdw_sapp_branch.json')
        self.df_customer = self.spark.read.option("multiLine", True).json('../origin_data/cdw_sapp_customer.json')
        self.df_credit = self.spark.read.option("multiLine", True).json('../origin_data/cdw_sapp_credit.json')
 # ---------------------------------------------------------------------------------------------------------------
    def transform(self) -> None:
        """
            Parameters explained: 
                None 

            Return values:
                None

            Explanation of function: 
                This function conducts the transformations noted below, they will be separated by dataframe:
                    ---------------- MAPPING DOCUMENT ----------------
                    BRANCH TABLE:

                        BRANCH_ZIP:
                            - If the source value is null load default (99999) value else Direct move.

                        BRANCH_PHONE:
                            - Change the format of phone number to (XXX)XXX-XXXX
                    
                    CREDIT TABLE: 

                        CREDIT_CARD_NO:
                            - Change target field name to ---> CUST_CC_NO
                    
                        DAY, MONTH, YEAR:
                            - Combine the three columns into one called ---> TIMEID and drop the other three. The format for
                              for the TIMEID is (YYYYMMDD)

                    CUSTOMER TABLE: 

                        FIRST_NAME:
                            - Convert the Name to Title Case

                        MIDDLE_NAME:
                            - Convert the middle name in lower case

                        LAST_NAME:
                            - Convert the Last Name in Title Case

                        STREET_NAME, APT_NO:
                            - Concatenate Apartment no and Street name of customer's Residence with comma as a separator (Street, Apartment)
                            - Drop STREET_NAME and APT_NO

                        CUST_PHONE:
                            - Change the format of phone number to (XXX)XXX-XXXX
                        
        """ 
        # Fill missing 'BRANCH_ZIP' with 99999
        self.df_branch = self.df_branch \
            .fillna({'BRANCH_ZIP': 99999}) \
            .withColumn(
                'BRANCH_ZIP', # cast the zip to a string and check if it is 5 digits long
                when(         # if it is cast it back to an int, otherwise set it to '99999'
                    length(col('BRANCH_ZIP').cast('string')) == 5,
                    col('BRANCH_ZIP').cast('int')
                ).otherwise(lit(99999))
            ) \
            .withColumn(
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
        # self.df_branch.show(10)

        # =========================================================================

        # rename the <old_column_name> with the <new_column_name>
        # combine the year, month, and day into timeid (format: YYYYMMDD)
        self.df_credit = self.df_credit \
            .withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO") \
            .withColumn(
                "TIMEID",
                concat(
                    lpad(col("YEAR").cast("string"), 4, "0"),
                    lpad(col("MONTH").cast("string"), 2, "0"),
                    lpad(col("DAY").cast("string"), 2, "0")
                ).cast("int")
            ) \
            .drop("YEAR", "MONTH", "DAY")
        # for testing purposes, make sure this is commented out for production
        # self.df_credit.show(10)

        # =========================================================================

        # Capitalize first and last name (only first letter upper)
        # make the middle name lower case
        # Combine street and apartment into full address (as string)
        # Format CUST_PHONE as (XXX)XXX-XXXX, only if it's exactly 10 digits because there is only 
        # 7-digits, the first three will be documented as 'XXX'
        self.df_customer = self.df_customer \
            .withColumn("FIRST_NAME", initcap(trim(col("FIRST_NAME")))) \
            .withColumn("MIDDLE_NAME", lower(trim(col("MIDDLE_NAME")))) \
            .withColumn("LAST_NAME", initcap(trim(col("LAST_NAME")))) \
            .withColumn(
                "FULL_STREET_ADDRESS",
                concat_ws(", ",
                    trim(col("STREET_NAME")),
                    trim(col("APT_NO").cast("string"))
                )
            ) \
            .drop("STREET_NAME","APT_NO") \
            .withColumn(
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
 # ---------------------------------------------------------------------------------------------------------------
    def load(self) -> None:
        """
            Parameters explained: 
                None 

            Return values:
                None

            Explanation of function: 
                 This function will load the transformed data into the target database to be used by the client. The
                 JDBC(Java Database Connectivity) connection properties are now declared an passed in the .jdbc() function
                 which writes the transform data directly into the target database.  
        """ 
        # MySQL JDBC connection properties
        url = "jdbc:mysql://localhost:3306/creditcard_capstone" 
        properties = {
            "user": "root",  
            "password": "password",  
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        # Write the data to MySQL 
        # CDW_SAPP_BRANCH
        self.df_branch.write.jdbc(url=url, table="CDW_SAPP_BRANCH", mode="append", properties=properties)
        # CDW_SAPP_CUSTOMER
        self.df_customer.write.jdbc(url=url, table="CDW_SAPP_CUSTOMER", mode="append", properties=properties)
        # CDW_SAPP_CREDIT_CARD
        self.df_credit.write.jdbc(url=url, table="CDW_SAPP_CREDIT_CARD", mode="append", properties=properties)
        # CDW_SAPP_LOAN_APPLICATION
        self.df_loan.write.jdbc(url=url, table="CDW_SAPP_LOAN_APPLICATION", mode="append", properties=properties)
 # ---------------------------------------------------------------------------------------------------------------
    def run(self) -> None:
        """
            Execution function: 
                This function is controlling the etl portion of the program, the format for this pipeline is Extract, 
                then Transform, and finally Load (ETL).
        """ 
        self.extract()
        self.transform()
        self.load()
        self.spark.stop()