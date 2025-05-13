import mysql.connector as dbconnection
import pandas as pd
# ----------------------------------------------------------------------------------------------
def display_menu(conn: object, cursor: object) -> None:
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
            customer_details(conn, cursor)
        elif res == '3':
            conn.close()
            break
        else:
            print() # added to give some extra white space
            print("Sorry that was an invalid entry please try again.")
            print() # added to give some extra white space
# ----------------------------------------------------------------------------------------------
def establish_db_conn() -> tuple:
    # make connection
    conn = dbconnection.connect(
        host='localhost',
        database='creditcard_capstone',
        user='root',
        password='password'
    )
    # make and return the cursor object
    return conn, conn.cursor()
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
def customer_details(conn: object, cursor: object) -> None:
    # Displaying customer info
    while True:
        try:
            # make 0 the sentinel value
            customer_ssn = input("Enter the customers 9-digit social security number to continue or 0 to return to the main menu: ")
            if len(customer_ssn) == 9:
                customer_ssn = int(customer_ssn)
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
    update_data = {}
    while True:
        options = {
            '3': "first_name", 
            '4': "middle_name", 
            '5': "last_name",
            '6': "credit_card_no",
            '7': "full_street_address",
            '8': "cust_city",
            '9': "cust_state",
            '10': "cust_country",
            '11': "cust_zip",
            '12': "cust_phone",
            '13': "cust_email"
        }
        try:
            print()# added for spacing and clarity
            print("Enter 0 at anytime to exit to the main menu without saving.") # make a sentinel value
            print("Enter 1 for information on the valid choices.")
            print("Enter 2 to update the record and commit your changes.")
            print()# added for spacing and clarity
            option = input("Enter the field you wish to update: ").strip()
            print()# added for spacing and clarity

            if option in options:
                # suggestions for formatting the data that has more structure.
                if option == '7':
                    print("Enter the address as follows (Street Name, Apt No)")
                elif option == '12':
                    print("The phone number format is as follows (XXX)XXX-XXXX") 

                new_val = input("Enter the new value you would like to change it to: ").strip()
            # match the option to the appropriate value and reserve it for the set clause
            if option in options:
                update_data[options[option]] = new_val
        except Exception as e:
            print()# added for spacing and clarity
            print("Error {}".format(e))
            print("Please try again...")
            print()# added for spacing and clarity
        # print(update_data) # for testing purposes
        # handling the other input values not contained in options
        if option == '0':
            break
        elif option == '1':
            for key, value in options.items():
                print("Option number: {} -> {}".format(key, value.replace('cust_', '').replace('_', ' ').title()))
        elif option == '2':
            # build the set clause dynamically
            set_clause = ", ".join([f"{key} = %s" for key in update_data.keys()])
            # print(set_clause) # for testing purposes
            values = list(update_data.values()) #these are the values for the set clause
            values.append(customer_ssn) # add this at the end of the list for the final %s in the where clause
            
            update_query = (
                """
                    update cdw_sapp_customer
                    set {set_clause}
                    where ssn = %s
                """.format(set_clause=set_clause))
            cursor.execute(update_query, values)
            conn.commit()
            break
# ----------------------------------------------------------------------------------------------
def main():
    conn, cursor = establish_db_conn()
    display_menu(conn, cursor)
# ----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
