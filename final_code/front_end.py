import mysql.connector as dbconnection
import pandas as pd
# ----------------------------------------------------------------------------------------------
def display_menu(conn: object, cursor: object) -> None:
    """
        Parameters explained: 
            conn: is a database connection object that will be passed to each function that needs it to perform its tasks.
            cursor: is the cursor object from the conn.cursor() function, this is passed to make execution calls. It is 
                    passed and not invoked at the beginning of each function (e.g. conn.cursor()) for simplicity and readability.
                    NOTE: The same results could have been achieved by simply passing the conn object and calling the cursor when needed.
        
        Return values:
            None

        Explanation of function:  
            This function is used to display the menu that will be the main display for the customer
            navigation options include: 
            '
                0) Quit
                1) Display Transaction Details
                2) Display Customer Details
                3) Display Monthly Bill for Card Number Entered
                4) Display Transactions Made by a Customer Between Two Dates
            '   
            The connection to the database is called in the main() function prior to calling this function 
            and it returns the connection and cursor objects that will be passed as needed to each of the 
            functions called. Refer to the documentation of each one to learn more. 
    """
    while True:
        # take in input from the menu while it is not '3' which is the sentinel value
        res = input(
        """ Please make a selection:
            0) Quit
            1) Display Transaction Details
            2) Display Customer Details
            3) Display Monthly Bill for Card Number Entered
            4) Display Transactions Made by a Customer Between Two Dates

            Enter your value here: """).strip() # add strip() to account for white space
        print() # added to give some extra white space

        if res == '1':
            transaction_details(cursor)
        elif res == '2':
            customer_details(conn, cursor)
        elif res == '3':
            monthly_bill_details(conn, cursor)
        elif res == '4':
            transactions_in_date_range(conn, cursor)
        elif res == '0':
            conn.close()
            break
        else:
            print() # added to give some extra white space
            print("Sorry that was an invalid entry please try again.")
            print() # added to give some extra white space
# ----------------------------------------------------------------------------------------------
def establish_db_conn() -> tuple:
    """
        Parameters explained: 
            None 

        Return values:
            Tuple which contains the connection object (conn) and the cursor object (conn.cursor()).

        Explanation of function: 
            This function is making the initial one-time connection to the database 'creditcard_capstone' and returning a 
            connection object and a cursor object that will be used to execute sql queries. 
    """
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
    """
        Parameters explained: 
            cursor: is the cursor object from the conn.cursor() function, this is passed to make execution calls.

        Return values:
            None

        Explanation of function: 
            This function is used to display the transaction details of a group of customers in a given zipcode and for a 
            specified month and year. The output is sorted in descending order by day and displays the following columns 
            ("Full Name", "Transaction ID", "Day of Purchase"). The function begins by running a while loop that will only 
            terminate when a valid zipcode is entered. The second while loop does the same and will only terminate when a 
            valid month and year are received. And finally the query is declared and ran, the results are then made into a 
            pandas dataframe for readability and displayed to the user.

    """
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

    # get the month and the year
    month, year = get_month_and_year()
        
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
    """
        Parameters explained: 
            conn: is the connection object used for committing the data in this case.
            cursor: is the cursor object from the conn.cursor() function, this is passed to make execution calls.

        Return values:
            None

        Explanation of function:
            This function is used to display the customer details from the cdw_sapp_customer table found in the 
            creditcard_capstone database. The first while loop is intended to display the customer details based
            on the social security number that is entered. When the data is retrieved from the database it is then
            converted to a pandas dataframe, displayed, and the while loop is terminated. The second while loop is the logic for updating the database
            dynamically based on the parameters the user would like to change. The user will have to enter the column they
            would like to change enter the value and submit it by entering in '2' in the menu. This will commit the 
            changes and break the loop which ends the function. 
    """
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
def monthly_bill_details(conn: object, cursor: object) -> None:
    """
        Parameters explained: 
            conn: is the connection object used for committing the data in this case.
            cursor: is the cursor object from the conn.cursor() function, this is passed to make execution calls.

        Return values:
            None

        Explanation of function:
            This function is used to query the database and generate a monthly bill for a given credit card number 
            and month's timeframe(month and year). 
    """
    while True: 
        # get the credit card number
        try:
            cc_num = int(input("Enter a valid 16-digit credit card number, do not add and hyphens or special characters (e.g. 4210653349028689)").strip())
        except Exception as e:
            print()# added for spacing and clarity
            print("Error {}".format(e))
            print("Please try again...")
            print()# added for spacing and clarity

        # get the month and the year
        month, year = get_month_and_year()
        # query the db for the desired output
        cursor.execute(
            """
                select transaction_type, sum(transaction_value) as amount
                from cdw_sapp_credit_card
                where month(timeid) = %s and year(timeid) = %s and cust_cc_no = %s
                group by transaction_type
                order by amount desc;
            """, (month, year, cc_num) 
        ) # make sure to inject these values in the order they are called in the query 

        # store the output in a variable and convert it to a dataframe
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=["Category", "Amount Spent"])
        # use the data from the data frame to get the total for the month and display the dataframe and the total to the user
        

    
# ----------------------------------------------------------------------------------------------
def transactions_in_date_range(conn: object, cursor: object) -> None:
    """
        Parameters explained: 
            conn: is the connection object used for committing the data in this case.
            cursor: is the cursor object from the conn.cursor() function, this is passed to make execution calls.

        Return values:
            None

        Explanation of function:
    """
    pass
# ----------------------------------------------------------------------------------------------
def get_month_and_year() -> tuple:
    # gather the input for the month and year from one input string
    while True:
        user_input = input(
            """Enter a valid month and year below
            NOTE: Use a 2-digit month followed by a 4-digit year, separated by a space (e.g., 05 1997):

            Month and Year: """
        ).strip()
        try:
            month_str, year_str = user_input.split() # splits the variables on the space
            if len(month_str) == 2 and len(year_str) == 4:
                month = int(month_str)
                year = int(year_str)
                break
            else:
                print("Invalid format. Make sure month is 2 digits and year is 4 digits.")
        except ValueError:
            print("Invalid input. Please enter the month and year separated by a space.")

    return month, year
# ----------------------------------------------------------------------------------------------
def main() -> None:
    """
        Parameters explained: 
            conn: is the connection object used for committing the data in this case.
            cursor: is the cursor object from the conn.cursor() function, this is passed to make execution calls.

        Return values:
            None

        Explanation of function:
    """
    conn, cursor = establish_db_conn()
    display_menu(conn, cursor)
# ----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
