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
            number = item.get('CUST_PHONE')
            item['CUST_PHONE'] = f"({number[:3]}){number[3:6]}-{number[6:]}"

    return data

# --------------------------------------------------------------------------------------------------------------------

def load(clean_data: list, file_extension: str) -> None:
    conn = mysql.connector.connect(
        host='localhost',
        user='your_username',
        password='your_password'
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
            BRANCH_ZIP VARCHAR(5),
            BRANCH_PHONE VARCHAR(13),
            LAST_UPDATED TIMESTAMP
        )
        """)
    elif file_extension == 'credit':
        pass
    elif file_extension == 'customer':
        pass

    # Insert data into table
    insert_query = "INSERT INTO users (name, age, email) VALUES (%s, %s, %s)" # and this ------------------------
    for row in clean_data:
        cursor.execute(insert_query, (row['name'], row['age'], row['email']))

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

# --------------------------------------------------------------------------------------------------------------------

def main():

    print("Beginning ETL")

    file_extensions = ["branch", "credit", "customer"]
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
