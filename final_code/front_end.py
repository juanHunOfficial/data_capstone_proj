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
            # transaction_details()
            print("1 works")
        elif res == '2':
            # customer_details()
            print("2 works")
        elif res == '3':
            break
        else:
            print() # added to give some extra white space
            print("Sorry that was an invalid entry please try again.")
            print() # added to give some extra white space


def main():
    display_menu()

if __name__ == "__main__":
    main()
