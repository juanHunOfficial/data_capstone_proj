import mysql.connector as dbconnection
import pandas as pd
# ----------------------------------------------------------------------------------------------
def display_menu(cursor: object) -> None:
    while True:
        # take in input from the menu while it is not '3' which is the sentinel value
        res = input(
        """ Please make a selection:
            1) Display Transaction Details
            2) Display Customer Details
            3) Quit

            Enter your value here: """).strip() # add strip() to account for white space
        print() # added to give some extra white space

        if res == '1':
            transaction_details(cursor)
        elif res == '2':
            customer_details(cursor)
            print("2 works")
        elif res == '3':
            break
        else:
            print() # added to give some extra white space
            print("Sorry that was an invalid entry please try again.")
            print() # added to give some extra white space
# ----------------------------------------------------------------------------------------------
def establish_db_conn() -> object:
    # make connection
    conn = dbconnection.connect(
        host='localhost',
        database='creditcard_capstone',
        user='root',
        password='password'
    )
    # make and return the cursor object
    return conn.cursor()
# ----------------------------------------------------------------------------------------------
def transaction_details(cursor: object) -> None:
    # .strip() will be added to each one to remove extra white space.
    while True:
        zipcode = input(
            """Enter a valid zipcode below
            NOTE: The valid format is a 5-digit number like: 99999. 
            
            Your value: """).strip()
        if len(zipcode) == 5:
            try:
                zipcode = int(zipcode) # in the db it is saved as a string, this is to check and make sure it is a valid number. 
                break
            except Exception as e:
                print() # Added for white space and clarity 
                print("Invalid zipcode entry, please try again...")
                print() # Added for white space and clarity 
        else:
            print() # Added for white space and clarity 
            print("Error, make sure your typing in a 5-digit value...")
            print() # Added for white space and clarity 

    while True:
        month = input(
            """Enter a valid month and year below
            NOTE: Use a 2-digit month followed by a 4-digit year like 05 for May and 1997 for the year.

            The month: """).strip()
        year = input("The year: ").strip()
        if len(month) == 2 and len(year) == 4:
            try:
                month = int(month)
                year = int(year)
                break
            except Exception as e:
                print() # Added for white space and clarity 
                print("Error: {e}".format(e))
                print("Please try again...")
                print() # Added for white space and clarity 
        else:
            print() # Added for white space and clarity 
            print("Error, make sure your month is 2-digits and your year is 4-digits with no extra characters.")
            print() # Added for white space and clarity 
    
    # query the db and retrieve a list of transactions made by customers in the specified zipcode for the given month and year
    # run query
    cursor.execute("""
        select concat(cu.first_name, " ", cu.last_name) as full_name , cc.transaction_id, day(cc.timeid)
        from cdw_sapp_credit_card cc
        left join cdw_sapp_customer cu on cc.cust_ssn = cu.ssn
        where cu.cust_zip = %s and month(timeid) = %s and year(timeid) = %s
        order by day(timeid) desc
    """, (zipcode, month, year))
    # retrieve the values and save them into a variable to be converted into a df
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Full Name", "Transaction ID", "Day of Purchase"])
    # only print if the df is filled
    if df.empty:
        print("No matching results for the given parameters.")
    else:
        print(df)
    print()# added for spacing and clarity
# ----------------------------------------------------------------------------------------------
def customer_details(cursor: object) -> None:
    # Displaying customer info
    while True:
        try:
            # make 0 the sentinel value
            customer_ssn = int(input("Enter the customers 9-digit social security number to continue or 0 to return to the main menu: ")) 
            if len(customer_ssn) == 9:
                cursor.execute("""
                    select * 
                    from cdw_sapp_customer
                    where ssn = %s
                """, (customer_ssn,))
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=[
                    "SSN", 
                    "First Name", 
                    "Middle Name", 
                    "Last Name",
                    "Credit Card No",
                    "Full Street Address",
                    "City",
                    "State",
                    "Country",
                    "Zip",
                    "Phone",
                    "Email",
                    "Last Updated"
                    ])
                # only print if the df is filled
                if df.empty:
                    print("No matching results for the given parameters.")
                else:
                    print(df)
                    break
                print()# added for spacing and clarity
        except Exception as e:
            print()# added for spacing and clarity
            print("Error {e}".format(e))
            print("Please try again...")
            print()# added for spacing and clarity

    # Updating customer info
    while True:
        valid_options = [
            "SSN", 
            "First Name", 
            "Middle Name", 
            "Last Name",
            "Credit Card No",
            "Full Street Address",
            "City",
            "State",
            "Country",
            "Zip",
            "Phone",
            "Email",
            "Last Updated"
        ]
        
        try:
            print("You can exit at any time by pressing 0")
            f_name = input("Enter the new first name: ").strip()
            m_name = input("Enter the new middle name: ").strip()
            l_name = input("Enter the new last name: ").strip()
            cc_number = input("Enter the new credit card number: ").strip()
            full_address = input("Enter the new full address(street, apt no): ").strip()
            city = input("Enter the new city: ").strip()
            state = input("Enter the new state: ").strip()
            country = input("Enter the new country: ").strip()
            zipcode = input("Enter the new zipcode: ").strip()
            phone = input("Enter the new phone: ").strip()
            email = input("Enter the new email: ").strip()
        except Exception as e:
            pass

# ----------------------------------------------------------------------------------------------
def main():
    cursor = establish_db_conn()
    display_menu(cursor)
# ----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
