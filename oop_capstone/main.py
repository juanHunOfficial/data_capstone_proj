from etl import Etl_Pipeline
from database_connector import DB_Connector
from visualizations import Visualizations

def main() -> None:
    # Display for clarity to the user on what is happening
    print("Moving the data through the pipeline, we will begin shortly...")
    # Make the connection and the tables
    db = DB_Connector(host='localhost', user='root', password='password')
    db.make_tables()
    # Make the pipeline and run it
    etl = Etl_Pipeline()
    etl.run()
    # Now with the data in place make the visuals
    visuals = Visualizations() # <----- revisit this and see if there is a way to simply call the ones you need in the menu
    # Display the menu and have it run as needed

if __name__ == "__main__":
    main()