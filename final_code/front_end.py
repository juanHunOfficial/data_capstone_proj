import mysql.connector as dbconnection
import pandas as pd
# ----------------------------------------------------------------------------------------------
def display_menu():
    while True:
        # take in input from the menu while it is not '3' which is the sentinel value
        res = input(
        """ Please make a selection:
            1) Display Transaction Details
            2) Display Customer Details
            3) Quit

            Enter your value here: """).strip() # add strip() to account for white space

        if res == '1':
            transaction_details()
        elif res == '2':
            # customer_details()
            print("2 works")
        elif res == '3':
            break
        else:
            print() # added to give some extra white space
            print("Sorry that was an invalid entry please try again.")
            print() # added to give some extra white space
# ----------------------------------------------------------------------------------------------
def transaction_details():
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
    # query the db and retrieve the data
    
    # make connection
    conn = dbconnection.connect(
        host='localhost',
        database='creditcard_capstone',
        user='root',
        password='password'
    )
    # make the cursor object
    cursor = conn.cursor()
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
    print(df)
    print()# added for spacing and clarity
# ----------------------------------------------------------------------------------------------
def main():
    display_menu()
# ----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
