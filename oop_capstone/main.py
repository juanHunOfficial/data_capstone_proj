from etl import Etl_Pipeline
from database_connector import DB_Connector

def main() -> None:
    # Make the connection and the tables
    db = DB_Connector(host='localhost', user='root', password='password')
    db.make_tables()
    # Make the pipeline and run it
    etl = Etl_Pipeline()
    etl.run()
    # Now with the data in place make the visuals

    # Display the menu and have it run as needed

if __name__ == "__main__":


    main()