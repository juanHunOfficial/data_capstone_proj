from etl import Etl_Pipeline
from database_connector import DB_Connector
from visualizations import Visualizations
from menu import Menu

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
    visuals = Visualizations(db.cursor) # <--- make separate menu for this and let it be called from the menu class.
    
    # Display the menu and have it run as needed
    menu = Menu(db.conn, db.cursor)
    menu.display_menu()
    
if __name__ == "__main__":
    main()