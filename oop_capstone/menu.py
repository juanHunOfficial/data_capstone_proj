import pandas as pd

class Menu:

    def __init__(self, conn: object, cursor: object) -> object: 
        """
            Constructor function:
                This is the Menu class' constructor. The connection and cursor are passed from the main.py.
                The options dictionary is used for accessing the primary methods that wil be used in this
                class, the helper methods are called as needed throughout these 4 main functions for easier
                troubleshooting.  
        """
        self.conn = conn
        self.cursor = cursor
        # NOTE: these are stored without the '()' so tht they do not get called upon initialization
        self.options = {
            '0' : self.close_connection,
            '1' : self.transaction_details,
            '2' : self.customer_details,
            '3' : self.monthly_bill_details,
            '4' : self.transactions_in_date_range
        }
 # ----------------------------------------------------------------------------------------------
    def close_connection(self) -> None:
        """
            Parameters explained: 
                None 

            Return values:
                None

            Explanation of function: 
                This function closes the connection to the database. 
        """ 

        self.conn.close()
 # ----------------------------------------------------------------------------------------------   
    def display_menu(self) -> None:
        """
            Parameters explained: 
                None
            
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
            try: 
                action = self.options.get(res)
                if action:
                    action()
            except:
                print("\nThat was an invalid response, please try again...\n")
            
            if res == '0':
                break
 # ----------------------------------------------------------------------------------------------
    def transaction_details(self) -> None:
        """
            Parameters explained: 
                None

            Return values:
                None

            Explanation of function: 
                This function is used to display the transaction details of a group of customers in a given zipcode and for a 
                specified month and year. The output is sorted in descending order by day and displays the following columns 
                ("Full Name", "Transaction ID", "Day of Purchase"). The function begins by running a while loop that will only 
                terminate when a valid zipcode is entered. Then the user will be prompted for a valid month and year in the 
                get_month_and_year() function. And finally the query is declared and ran, the results are then made into a 
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
                    print("\nInvalid zipcode entry, please try again...\n")
            else:
                print("\nError, make sure your typing in a 5-digit value...\n")

        # get the month and the year
        year, month, _ = Menu.get_year_month_day() # the last variable is for the day but because we are not using it an '_' was used
            
        # query the db and retrieve a list of transactions made by customers in the specified zipcode for the given month and year
        # run query
        self.cursor.execute("""
            select concat(cu.first_name, " ", cu.last_name) as full_name , cc.transaction_id, day(cc.timeid)
            from cdw_sapp_credit_card cc
            left join cdw_sapp_customer cu on cc.cust_ssn = cu.ssn
            where cu.cust_zip = %s and month(timeid) = %s and year(timeid) = %s
            order by day(timeid) desc
        """, (zipcode, month, year))
        # retrieve the values and save them into a variable to be converted into a df
        data = self.cursor.fetchall()
        df = pd.DataFrame(data, columns=["Full Name", "Transaction ID", "Day of Purchase"])
        # only print if the df is filled
        if df.empty:
            print("\nNo matching results for the given parameters.\n")
        else:
            print(f"\n{df}\n")
    # ----------------------------------------------------------------------------------------------
    def customer_details(self) -> None:
        """
            Parameters explained: 
                None

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
                customer_ssn = input("Enter the customers 9-digit social security number to continue or 0 to return to the main menu(e.g. 123459506): ")
                if len(customer_ssn) == 9:
                    customer_ssn = int(customer_ssn)
                    self.cursor.execute("""
                        select * 
                        from cdw_sapp_customer
                        where ssn = %s
                    """, (customer_ssn,))
                    data = self.cursor.fetchall()
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
                        print("\nNo matching results for the given parameters.\n")
                    else:
                        print(f"\n{df}\n")
                        break
            except Exception as e:
                print("\nError {e}".format(e))
                print("Please try again...\n")

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
                print("\nEnter 0 at anytime to exit to the main menu without saving.") # make a sentinel value
                print("Enter 1 for information on the valid choices.")
                print("Enter 2 to update the record and commit your changes.\n")
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
                print("\nError {}".format(e))
                print("Please try again...\n")
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
                self.cursor.execute(update_query, values)
                self.conn.commit()
                break
# ----------------------------------------------------------------------------------------------
    def monthly_bill_details(self) -> None:
        """
            Parameters explained: 
                None

            Return values:
                None

            Explanation of function:
                This function is used to query the database and generate a monthly bill for a given credit card number 
                and month's timeframe(month and year). The first function will prompt the user for a valid credit card number in 
                the get_credit_card_number() function. Then the user will be prompted for a valid month and year in the get_month_and_year()
                function. Finally the database is queried and converted into a pandas dataframe, where we will extract the total
                spent for the timeframe specified the breakdown in the form of the dataframe. 
        """
        # get the credit card number
        cc_num = Menu.get_credit_card_number()

        # get the month and the year
        year, month, _ = Menu.get_year_month_day() # the last variable is for the day but because we are not using it an '_' was used
        # query the db for the desired output
        self.cursor.execute(
            """
                select transaction_type, sum(transaction_value) as amount
                from cdw_sapp_credit_card
                where month(timeid) = %s and year(timeid) = %s and cust_cc_no = %s
                group by transaction_type
                order by amount desc;
            """, (month, year, cc_num) 
        ) # make sure to inject these values in the order they are called in the query 

        # store the output in a variable and convert it to a dataframe
        data = self.cursor.fetchall()
        df = pd.DataFrame(data, columns=["Category", "Amount Spent"])
        # use the data from the data frame to get the total for the month and display the dataframe and the total to the user
        total_amount = df['Amount Spent'].sum()

        # only print if the df is filled
        if df.empty:
            print("\nNo matching results for the given parameters.\n")
        else:
            print(f"\nThe total amount for spent in {month}/{year} is: ${total_amount}\nHere's the break down:")
            print(f"\n{df}\n")
# ----------------------------------------------------------------------------------------------
    def transactions_in_date_range(self) -> None:
        """
            Parameters explained: 
                None

            Return values:
                None

            Explanation of function:
                This function is used to display the transactions made by a customer between two dates ordered by year, 
                month, and day in descending order.
        """
        # get the credit card number
        cc_num = Menu.get_credit_card_number()
        while True:
            # get the year, month, and day for both dates and assign them custom variables
            print("Enter the first date for the range")
            year, month, day = Menu.get_year_month_day()
            first_timeid = int(f"{year}{month}{day}")

            print("Enter the second date for the range")
            year, month, day = Menu.get_year_month_day()
            second_timeid = int(f"{year}{month}{day}")

            # check if the date range is good else prompt the user again
            if first_timeid < second_timeid:
                # cast the dates back into string from for the query, sql will not take them in integer format
                first_timeid = str(first_timeid)
                second_timeid = str(second_timeid)
                break
            else:
                print("\nLooks like you enter the dates backwards, ensure to enter the earlier date first and then the later date...\n")

        # print(first_timeid, " ", second_timeid)# for testing purposes, make sure this is commented out during production

        # query the db for the desired output
        self.cursor.execute(
            """
                select transaction_id, transaction_type, transaction_value, timeid
                from cdw_sapp_credit_card
                where cust_cc_no =  %s and timeid between %s and %s
                order by year(timeid) desc, month(timeid) desc, day(timeid) desc;
            """, (cc_num, first_timeid, second_timeid)
        )
        # store the output in a variable and convert it to a dataframe
        data = self.cursor.fetchall()
        df = pd.DataFrame(data, columns=["Transaction ID", "Category", "Transaction Amount", "Date of Purchase"])
        
        # display the data
        print(f"These are all the transaction from {first_timeid} - {second_timeid}: ")
        print(f"\n{df}\n")
# ----------------------------------------------------------------------------------------------
    def get_year_month_day(self) -> tuple:
        """
            Parameters explained: 
                None

            Return values:
                This function returns a tuple which contains the year, month, and day variables.

            Explanation of function:
                This function was created because multiple locations required the same logic to be used. Therefore this 
                while loop will prompt the user for the year, month, and day separated by a space and will only return when 
                a valid year, month, and day are received. 
        """
        # Gather the input for year, month, and day from one input string
        while True:
            user_input = input(
                """Enter a valid date below
                NOTE: Use a 4-digit year, 2-digit month, and 2-digit day separated by spaces (e.g., 2023 05 14):

                Year Month Day: """
            ).strip()
            try:
                year, month, day = user_input.split()  # Split on space
                if len(year) == 4 and len(month) == 2 and len(day) == 2:
                    return year, month, day
                else:
                    print("\nInvalid format. Make sure year is 4 digits, month and day are 2 digits each.\n")
            except ValueError:
                print("\nInvalid input. Please enter year, month, and day separated by spaces.\n")
# ----------------------------------------------------------------------------------------------
    def get_credit_card_number(self) -> int:
        """
            Parameters explained: 
                None

            Return values:
                This function returns an integer value containing the valid credit card number. 

            Explanation of function:
                This function was created because multiple locations required the same logic to be used. Therefore this 
                while loop will check to make sure that a valid credit card has been entered. When an input is 16 characters 
                long the variable will be converted to an integer to make sure it is a number and return the value.
        """
        while True: 
            # get the credit card number
            try:
                cc_num = input("Enter a valid 16-digit credit card number, do not add and hyphens or special characters (e.g. 4210653349028689): ").strip()
                if len(cc_num) == 16:
                    cc_num = int(cc_num) # this is for validation purposes, to make sure the value is a whole number
                    return cc_num # if everything passes return the credit card number
            except Exception as e:
                print("\nError {}".format(e))
                print("Please try again...\n")
# ----------------------------------------------------------------------------------------------