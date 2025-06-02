from etl import Etl_Pipeline
from database_connector import DB_Connector
from menu import Menu

def main() -> None:
    # Display for clarity to the user on what is happening
    print("Moving the data through the pipeline, we will begin shortly...")
    # Make the connection and the tables
    print("Connecting to the db...")
    db = DB_Connector(host='localhost', user='root', password='password')
    db.make_tables()
    # Make the pipeline and run it
    print("Running the pipeline...")
    etl = Etl_Pipeline()
    etl.run()
    # Visualizations went here but it has been removed because now it is all done in tableau
    print(""" ADDED FOR VISUAL CLARITY ON THE CLI





            """)
    # Display the menu and have it run as needed
    menu = Menu(db.conn, db.cursor)
    menu.display_menu()
    
if __name__ == "__main__":
    main()