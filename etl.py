import json
import mysql.connector as dbconnection

# --------------------------------------------------------------------------------------------------------------------

def extract(file_extension: str) -> list:

    with open(f'origin_data/cdw_sapp_{file_extension}.json', 'r' ) as f:
        data = json.load(f)
    
    return data

# --------------------------------------------------------------------------------------------------------------------

def transform(data: list, file_extension: str) -> list:
    
    if file_extension == 'branch':
        for item in data:
            if not item.get('BRANCH_ZIP'):
                item['BRANCH_ZIP'] = 99999
            number = item.get('BRANCH_PHONE')
            item['BRANCH_PHONE'] = f"({number[:3]}){number[3:6]}-{number[6:]}"
    
    elif file_extension == "credit":
        for item in data:
            item['TIMEID'] = int(f"{item.get('YEAR')}{item.get('MONTH')}{item.get('DAY')}")
    
    elif file_extension == 'customer':
        for item in data:
            first_name, middle_name, last_name = item.get('FIRST_NAME').strip(), item.get('MIDDLE_NAME').strip(), item.get('LAST_NAME').strip()
            item['FIRST_NAME'] = f"{first_name[0].upper()}{first_name[1:]}"
            item['MIDDLE_NAME'] = f"{middle_name.lower()}"
            item['LAST_NAME'] = f"{last_name[0].upper()}{last_name[1:]}"
            item['FULL_STREET_ADDRESS'] = f'{item.get('STREET_NAME')}, {item.get('APT_NO')}'
            number = str(item.get('CUST_PHONE'))
            item['CUST_PHONE'] = f"({number[:3]}){number[3:6]}-{number[6:]}"

    return data

# --------------------------------------------------------------------------------------------------------------------

def load(clean_data: list, file_extension: str) -> None:
    conn = dbconnection.connect(
        host='localhost',
        user='root',
        password='password'
    )
    cursor = conn.cursor()

    # Create database if not exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
    cursor.execute("USE creditcard_capstone")

    # Create tables if not exists
    if file_extension == 'branch':
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
                BRANCH_CODE INT AUTO_INCREMENT PRIMARY KEY,
                BRANCH_NAME VARCHAR(100),
                BRANCH_STREET VARCHAR(100),
                BRANCH_CITY VARCHAR(50),
                BRANCH_STATE VARCHAR(50), 
                BRANCH_ZIP VARCHAR(10),
                BRANCH_PHONE VARCHAR(13),
                LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        # Insert data into table
        insert_query = """
            INSERT INTO CDW_SAPP_BRANCH (
                BRANCH_CODE, 
                BRANCH_NAME, 
                BRANCH_STREET,
                BRANCH_CITY,
                BRANCH_STATE,
                BRANCH_ZIP,
                BRANCH_PHONE,
                LAST_UPDATED
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in clean_data:
            cursor.execute(insert_query, (
                row['BRANCH_CODE'], 
                row['BRANCH_NAME'], 
                row['BRANCH_STREET'],
                row['BRANCH_CITY'], 
                row['BRANCH_STATE'], 
                row['BRANCH_ZIP'],
                row['BRANCH_PHONE'], 
                row['LAST_UPDATED']
            ))
    # -----------------------------------------------------------------------------------
    elif file_extension == 'customer':
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
        # Insert data into table
        insert_query = """
            INSERT INTO CDW_SAPP_CUSTOMER (
                SSN, 
                FIRST_NAME, 
                MIDDLE_NAME,
                LAST_NAME,
                CREDIT_CARD_NO,
                FULL_STREET_ADDRESS,
                CUST_CITY,
                CUST_STATE,
                CUST_COUNTRY,
                CUST_ZIP,
                CUST_PHONE,
                CUST_EMAIL,
                LAST_UPDATED 
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)
        """
        for row in clean_data:
            cursor.execute(insert_query, (
                row['SSN'], 
                row['FIRST_NAME'], 
                row['MIDDLE_NAME'],
                row['LAST_NAME'], 
                row['CREDIT_CARD_NO'], 
                row['FULL_STREET_ADDRESS'],
                row['CUST_CITY'], 
                row['CUST_STATE'],
                row['CUST_COUNTRY'], 
                row['CUST_ZIP'],
                row['CUST_PHONE'], 
                row['CUST_EMAIL'],
                row['LAST_UPDATED']
            ))
        # -----------------------------------------------------------------------------------
    elif file_extension == 'credit':
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
        # Insert data into table
        insert_query = """
            INSERT INTO CDW_SAPP_CREDIT_CARD (
                TRANSACTION_ID, 
                CUST_CC_NO, 
                TIMEID,
                CUST_SSN,
                BRANCH_CODE,
                TRANSACTION_TYPE,
                TRANSACTION_VALUE
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for row in clean_data:
            cursor.execute(insert_query, (
                row['TRANSACTION_ID'], 
                row['CREDIT_CARD_NO'], 
                row['TIMEID'],
                row['CUST_SSN'], 
                row['BRANCH_CODE'], 
                row['TRANSACTION_TYPE'],
                row['TRANSACTION_VALUE']
            ))
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

# --------------------------------------------------------------------------------------------------------------------

def main():

    print("Beginning ETL")

    file_extensions = ["branch", "customer", "credit" ]
    for extension in file_extensions:

        print("Extraction Beginning.")
        data = extract(extension)
        print("Extraction Complete.")

        print("Transformation Beginning.")
        clean_data = transform(data, extension)
        print("Transformation Complete.")

        print("Loading Beginning.")
        load(clean_data, extension)
        print("Loading Complete.")

    print("ETL Complete.")


if __name__ == '__main__':
    main()