import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import calendar

class Visualizations:

    def __init__(self, cursor: object) -> object:
        """
            Constructor function:
                This is the Visualizations class' constructor. The intent is each one of these functions will
                be called as needed and displayed for the client to see.  
        """
        # (only the cursor is needed for the visualizations)
        self.cursor = cursor
        # now select the database you wish to use.
        self.cursor.execute("use creditcard_capstone")
 # ----------------------------------------------------------------------------------------------
    def plot_transaction_count_data(self) -> None:
        """
            Problem statement: 
                Calculate and plot which transaction type has the highest transaction count.
        """
        self.cursor.execute(
            """
                select transaction_type, count(transaction_id) as orders, sum(transaction_value)
                from cdw_sapp_credit_card
                group by transaction_type
                order by orders; 
            """
        )
        # convert to a dataframe 
        df = pd.DataFrame(self.cursor.fetchall(), columns=["Category", "Amount of Orders", "Total of Orders"])
        # setup my bar plot
        sns.set_theme(style='whitegrid')
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(data=df, x="Category", y="Amount of Orders", palette="crest")

        # add data labels
        for container in ax.containers:
            ax.bar_label(container, fmt='%d')

        plt.title("Amount of Orders by Category", fontsize=14)
        plt.xticks(rotation=45)
        plt.grid(True, linestyle=':', linewidth=1, color='gray') 
        plt.tight_layout()
        plt.show()
 # ----------------------------------------------------------------------------------------------
    def plot_customer_density(self) -> None:
        """
            Problem Statement:
                Calculate and plot the top 10 states with the highest number of customers.
        """
        self.cursor.execute(
            """
                select cust_state, count(ssn)
                from cdw_sapp_customer
                group by cust_state
                order by count(ssn) desc
                limit 10;
            """
        )

        df = pd.DataFrame(self.cursor.fetchall(), columns=["State", "Number of Customers"])
        # bar plot of customers per state
        plt.figure(figsize=(10, 6))
        sns.barplot(data=df, x="State", y="Number of Customers", palette="viridis")
        plt.xlabel(xlabel="States")
        plt.title("Number of Customers by State")
        plt.grid(True, linestyle=':', linewidth=1, color='gray') 
        plt.tight_layout()
        plt.show()
 # ----------------------------------------------------------------------------------------------
    def display_top_10_customers(self) -> None:
        """
            Problem Statement:
                Calculate the total transaction sum for each customer based on their individual transactions.
                Identify the top 10 customers with the highest transaction amounts (in dollar value). Create 
                a plot to showcase these top customers and their transaction sums.
        """
        self.cursor.execute(
            """
                select cu.first_name, cu.last_name, sum(cc.transaction_value) as total
                from cdw_sapp_credit_card cc
                left join cdw_sapp_customer cu on cc.cust_ssn = cu.ssn
                group by cust_ssn
                order by total desc
                limit 10;
            """
        )
        # convert to a dataframe 
        df = pd.DataFrame(self.cursor.fetchall(), columns=["First Name", "Last Name", "Total"])
        # combine to get the person's full name
        df["Customer"] = df["First Name"] + " " + df["Last Name"]

        # plot
        plt.figure(figsize=(10, 6))
        sns.barplot(data=df, y="Customer", x="Total", palette="Blues_d")
        plt.title("Top 10 Customers by Total Spending")
        plt.xlabel("Total Spent ($)")
        plt.ylabel("Customer")
        plt.grid(True, linestyle=':', linewidth=1, color='gray') 
        plt.tight_layout()
        plt.show()
 # ----------------------------------------------------------------------------------------------
    def display_self_employed_apps(self) -> None:
        """
            Problem Statement:
                Calculate and plot the percentage of applications approved for self-employed applicants. 
                Use the appropriate chart or graph to represent this data.
        """
        self.cursor.execute(
            """
                select self_employed
                from cdw_sapp_loan_application
                order by self_employed;
            """
        )

        df = pd.DataFrame(self.cursor.fetchall(), columns=["Self Employed"])
        counts = df["Self Employed"].value_counts()

        # Create pie chart
        plt.figure(figsize=(6, 6))
        plt.pie(
            counts,
            labels=counts.index,
            autopct='%1.1f%%',
            startangle=90,
            colors=["#5e60ce", "#ffbe0b"],  # green for yes, red for no
            wedgeprops={'edgecolor': 'black'}
        )

        plt.title("Self-Employed vs Not Self-Employed")
        plt.axis('equal') # keeps it circular
        plt.tight_layout()
        plt.show()
 # ----------------------------------------------------------------------------------------------
    def plot_rejected_male_apps(self) -> None:
        """
            Problem Statement:
                Calculate the percentage of rejection for married male applicants. Use the ideal 
                chart or graph to represent this data.
        """
        self.cursor.execute(
            """
                select application_status
                from cdw_sapp_loan_application
                where gender = 'male' and married = 'Yes';
            """
        )

        df = pd.DataFrame(self.cursor.fetchall(), columns=["Status"])
        # map 'Y' and 'N' to readable labels
        df['Claim Status'] = df['Status'].replace({'Y': 'Approved', 'N': 'Denied'})

        # count values
        status_counts = df['Claim Status'].value_counts()

        # plot pie chart
        colors = ["#4CAF50", "#F44336"]  # green for approved, red for denied

        plt.figure(figsize=(6, 6))
        plt.pie(
            status_counts,
            labels=status_counts.index,
            autopct='%1.1f%%',
            startangle=90,
            colors=colors,
            wedgeprops={'edgecolor': 'black'}
        )

        plt.title("Approval Percentage of Married Men", fontsize=14)
        plt.axis('equal') 
        plt.tight_layout()
        plt.show()
 # ----------------------------------------------------------------------------------------------
    def plot_top_three_months(self) -> None: # < ------------------------ change this one
        """
            Problem Statement:
                Calculate and plot the top three months with the largest volume of transaction data. 
                Use the ideal chart or graph to represent this data.
        """
        # count(transaction_id) was used over sum(transaction_value) because the question was asking for volume not sum or revenue etc. 
        self.cursor.execute(
            """
                select month(timeid), count(transaction_id) as total
                from cdw_sapp_credit_card
                group by month(timeid)
                order by total desc
                limit 3;
            """
        )

        df = pd.DataFrame(self.cursor.fetchall(), columns=["Month", "Total Transactions"])
        # convert the numbers returned to months in place
        df['Month'] = df['Month'].apply(lambda x: calendar.month_name[int(x)])

        plt.figure(figsize=(8, 5))

        # draw stems
        plt.hlines(y=df["Month"], xmin=0, xmax=df["Total Transactions"], color='gray', linewidth=2)

        # draw dots
        plt.plot(df["Total Transactions"], df["Month"], "o", color="teal", markersize=10)

        # labels and title
        plt.title("Top 3 Months by Total Transactions")
        plt.xlabel("Total Transactions")
        plt.ylabel("Month")
        plt.tight_layout()
        plt.show()
 # ----------------------------------------------------------------------------------------------
    def plot_healthcare_transactions(self) -> None: # < ------------ change this one
        """
            Problem Statement:
                Calculate and plot which branch processed the highest total dollar value of 
                healthcare transactions. Use the ideal chart or graph to represent this data.
        """
        self.cursor.execute(
            """
                select cb.branch_code, sum(cc.transaction_value)
                from cdw_sapp_credit_card cc 
                left join cdw_sapp_branch cb on cc.branch_code = cb.branch_code
                where cc.transaction_type = 'Healthcare'
                group by cb.branch_code
                order by sum(cc.transaction_value) desc;
            """
        )

        df = pd.DataFrame(self.cursor.fetchall(), columns=["Branch Code", "Total Spent on Healthcare"])

        df_sorted = df.sort_values("Total Spent on Healthcare", ascending=False)

        top_spender = df_sorted.iloc[0]

        # create a column that highlighting the top spender
        df_sorted['highlight'] = df_sorted['Branch Code'] == top_spender['Branch Code']

        # plot the horizontal bar chart
        plt.figure(figsize=(10, 6))
        sns.barplot(
            data=df_sorted,
            x="Branch Code",
            y="Total Spent on Healthcare",
            hue="highlight",  # use hue to highlight the top spender
            palette={True: "green", False: "lightgray"},  # highlight the top spender in green
            dodge=False
        )

        # title and labels
        plt.title("Top Spending Branch in Healthcare")
        plt.xlabel("Branch Code")
        plt.ylabel("Total Spent on Healthcare")
        plt.tight_layout()
        # Show the plot
        plt.show()
 # ----------------------------------------------------------------------------------------------
