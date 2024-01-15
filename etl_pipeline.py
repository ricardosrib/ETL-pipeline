import pandas as pd
import mysql.connector
from datetime import datetime

# Global variables for database connection

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = ''
OLTP_DB_NAME = 'oltp'
OLAP_DB_NAME = 'olap'


# Create Databases OLTP and OLAP

# Establish the connection to the MySQL server
cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    password = DB_PASSWORD
)

# Create a cursor object to execute SQL queries
cursor = cnx.cursor()

# Create the OLTP database
create_oltp_db_query = "CREATE DATABASE IF NOT EXISTS oltp"
cursor.execute(create_oltp_db_query)

# Create the OLAP database
create_olap_db_query = "CREATE DATABASE IF NOT EXISTS olap"
cursor.execute(create_olap_db_query)

# Commit the database creation
cnx.commit()

# Close the cursor and connection
cursor.close()
cnx.close()



# Load tables in OLTP

# Establish the connection to the MySQL database
cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLTP_DB_NAME,
    password = DB_PASSWORD
)

# Create a cursor object to execute SQL queries
cursor = cnx.cursor()

# Define the paths to the Excel files
excel_files = [
    r'\Bookshop_data.xlsx',
    r'\BookshopLibraries.xlsx'
]

# Define a mapping of data types from Excel to MySQL
data_type_mapping = {
    'int64': 'INT',
    'float64': 'FLOAT',
    'object': 'VARCHAR(255)',  # Adjust the length as per your needs
    'datetime64': 'DATE'  # Use DATE data type for the 'Birthday' column
}

# Iterate over each Excel file
for excel_file in excel_files:
    # Load the Excel data
    xls = pd.ExcelFile(excel_file)

    # Iterate over each sheet in the Excel file
    for sheet_name in xls.sheet_names:
        # Load the sheet data into a Pandas DataFrame
        excel_data = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Skip specific sheets if needed
        # if excel_file == 'Bookshop_data.xlsx' and sheet_name in ['Info', 'Edition']:
        #     continue

        # Convert the DataFrame to a list of dictionaries
        data = excel_data.to_dict(orient='records')

        # Generate the SQL query to create the table
        table_name = sheet_name.replace(' ', '_')  # Replace spaces with underscores
        columns = ', '.join(f"`{col}` {data_type_mapping.get(str(excel_data[col].dtype), 'VARCHAR(255)')}" for col in excel_data.columns)  # Enclose column names in backticks and specify data types
        print(f"Table: {table_name}, Columns: {columns}")  # Print table and column names for debugging

        # Generate the SQL query to create the table
        query_create_table = f"CREATE TABLE `{table_name}` ({columns})"
        print(f"Create Table Query: {query_create_table}")

        # Execute the SQL query to create the table
        cursor.execute(query_create_table)

        # Generate the SQL query to insert data into the table
        placeholders = ', '.join([f"%({col})s" for col in excel_data.columns if col != 'Staff Comment' and col != 'Print run size'])  # Exclude 'Staff Comment' and 'Print run size' columns
        query_insert_data = f"INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in excel_data.columns if col != 'Staff Comment' and col != 'Print run size')}) VALUES ({placeholders})"
        print(f"Insert Data Query: {query_insert_data}")

        # Convert timestamp values to string representation
        for row in data:
            for col, value in row.items():
                if isinstance(value, pd.Timestamp):
                    if col == 'Birthday':
                        row[col] = value.date().strftime('%Y-%m-%d')  # Keep only the date part
                    else:
                        row[col] = str(value)

        # Execute the SQL query to insert data into the table
        cursor.executemany(query_insert_data, data)

# Commit the changes to the database
cnx.commit()

# Close the cursor and connection
cursor.close()
cnx.close()




# ETL


# Book dimension

# Connect to the OLTP database
cnx_oltp = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    password = DB_PASSWORD,
    database = OLTP_DB_NAME
)

# Create a cursor object for the OLTP database
cursor_oltp = cnx_oltp.cursor()

# Connect to the OLAP database
cnx_olap = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    password = DB_PASSWORD,
    database = OLAP_DB_NAME
)

# Create a cursor object for the OLAP database
cursor_olap = cnx_olap.cursor()



# Retrieve data from the OLTP tables using SQL JOINs
query = """
    SELECT
        b.BookID,
        b.Title,
        CONCAT(a.`First name`, ' ', a.`Last name`) AS AuthorName,
        i.Genre,
        e.Pages
    FROM
        Book AS b
        JOIN Author AS a ON b.AuthID = a.AuthID
        JOIN (
            SELECT CONCAT(BookID1, BookID2) AS BookID, Genre
            FROM Info
        ) AS i ON b.BookID = i.BookID
        JOIN Edition AS e ON b.BookID = e.BookID
"""

# Execute the query
cursor_oltp.execute(query)

# Fetch all the rows from the query result
book_dim_data = cursor_oltp.fetchall()

# Create the Book dimension table in the OLAP database
query_create_book_dim = """
    CREATE TABLE Book (
        BookID VARCHAR(255),
        Title VARCHAR(255),
        AuthorName VARCHAR(255),
        Genre VARCHAR(255),
        Pages INT
    )
"""
cursor_olap.execute(query_create_book_dim)

# Insert data into the Book dimension table
query_insert_book_dim = "INSERT INTO Book (BookID, Title, AuthorName, Genre, Pages) VALUES (%s, %s, %s, %s, %s)"
cursor_olap.executemany(query_insert_book_dim, book_dim_data)

# Commit the changes to the OLAP database
cnx_olap.commit()

# Close the cursors and connections
cursor_oltp.close()
cnx_oltp.close()

cursor_olap.close()
cnx_olap.close()



# Time dimension

# Establish connections to OLTP and OLAP databases
oltp_cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLTP_DB_NAME,
    password = DB_PASSWORD
)

olap_cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLAP_DB_NAME,
    password = DB_PASSWORD
)

# Create cursor objects for OLTP and OLAP connections
oltp_cursor = oltp_cnx.cursor()
olap_cursor = olap_cnx.cursor()


# Query to select Sale date and OrderID from Sales Q1, Q2, Q3, and Q4 sheets
sale_query = "SELECT `Sale date`, `OrderID` FROM Sales_Q1 UNION ALL " \
             "SELECT `Sale date`, `OrderID` FROM Sales_Q2 UNION ALL " \
             "SELECT `Sale date`, `OrderID` FROM Sales_Q3 UNION ALL " \
             "SELECT `Sale date`, `OrderID` FROM Sales_Q4"

# Execute query on OLTP connection
oltp_cursor.execute(sale_query)
result_sales = oltp_cursor.fetchall()

# Extract sale dates and order IDs
sale_dates = [str(row[0]).split()[0] for row in result_sales]
order_ids = [str(row[1]) for row in result_sales]

# Create time dimension table in OLAP database
create_table_query = "CREATE TABLE Time (OrderID VARCHAR(255), SaleDate DATE, Year INT, Month INT, Day INT)"
olap_cursor.execute(create_table_query)
olap_cnx.commit()

# Insert data into the time dimension table
insert_query = "INSERT IGNORE INTO Time (OrderID, SaleDate, Year, Month, Day) VALUES (%s, %s, %s, %s, %s)"
time_data = [(order_id, sale_date, datetime.strptime(sale_date, "%Y-%m-%d").year, datetime.strptime(sale_date, "%Y-%m-%d").month, datetime.strptime(sale_date, "%Y-%m-%d").day) for order_id, sale_date in zip(order_ids, sale_dates)]
olap_cursor.executemany(insert_query, time_data)
olap_cnx.commit()

# Close cursor and connections
oltp_cursor.close()
olap_cursor.close()
oltp_cnx.close()
olap_cnx.close()



# Store dimension

# Establish the connection to the OLTP MySQL database
oltp_cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLTP_DB_NAME,
    password = DB_PASSWORD
)

# Create a cursor object to execute OLTP SQL queries
oltp_cursor = oltp_cnx.cursor()

# Establish the connection to the OLAP MySQL database
olap_cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLAP_DB_NAME,
    password = DB_PASSWORD
)

# Create a cursor object to execute OLAP SQL queries
olap_cursor = olap_cnx.cursor()

# Define the query to extract the required columns from the LibraryProfile sheet
query = "SELECT CAST(`Library ID` AS CHAR), Library, `Number of staff` FROM LibraryProfile"

# Execute the query on the OLTP database
oltp_cursor.execute(query)

# Fetch all the records from the query result
result = oltp_cursor.fetchall()

# Convert the query result to a Pandas DataFrame
df = pd.DataFrame(result, columns=['LibraryID', 'Library', 'Number of staff'])

# Define the table name in the OLAP database
table_name = 'Store'

# Create the table in the OLAP database if it doesn't exist
olap_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (`LibraryID` VARCHAR(255), Library VARCHAR(255), `Number of staff` INT)")

# Convert the DataFrame to a list of tuples
data = [tuple(row) for row in df.values]

# Prepare the SQL query to insert data into the table
query = f"INSERT INTO {table_name} (LibraryID, Library, `Number of staff`) VALUES (%s, %s, %s)"

# Execute the query on the OLAP database
olap_cursor.executemany(query, data)

# Commit the changes to the OLAP database
olap_cnx.commit()

# Close the cursors and connections
oltp_cursor.close()
oltp_cnx.close()
olap_cursor.close()
olap_cnx.close()



# Sales fact

# Establish the connection to the OLTP MySQL database
oltp_cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLTP_DB_NAME,
    password = DB_PASSWORD
)

# Create a cursor object to execute OLTP SQL queries
oltp_cursor = oltp_cnx.cursor()

# Establish the connection to the OLAP MySQL database
olap_cnx = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    database = OLAP_DB_NAME,
    password = DB_PASSWORD
)

# Create a cursor object to execute OLAP SQL queries
olap_cursor = olap_cnx.cursor()

# Create the Sales fact table in the OLAP database
query_create_sales_fact = """
    CREATE TABLE Sales_fact (
        BookID VARCHAR(255),
        OrderID VARCHAR(255),
        `Library ID` VARCHAR(255),
        `Number of Checkouts` INT,
        Price FLOAT,
        `Total Sales` FLOAT,
        UNIQUE (BookID, OrderID, `Library ID`)
    )
"""

olap_cursor.execute(query_create_sales_fact)

# Insert data into the Sales fact table, calculating 'Total Sales'
query_insert_book_fact = """
    INSERT IGNORE INTO olap.Sales_fact (BookID, OrderID, `Library ID`, `Number of Checkouts`, Price, `Total Sales`)
    SELECT DISTINCT
        e.BookID,
        sq.OrderID,
        lib.`Library ID`,
        c.`Number of Checkouts`,
        e.Price,
        c.`Number of Checkouts` * e.Price AS `Total Sales`
    FROM
        oltp.Edition AS e
        JOIN oltp.Checkouts AS c ON e.BookID = c.BookID
        JOIN oltp.Catalog AS cat ON e.ISBN = cat.ISBN
        JOIN oltp.LibraryProfile AS lib ON cat.`Library ID` = lib.`Library ID`
        JOIN (
        SELECT OrderID, ISBN FROM oltp.Sales_Q1
        UNION ALL
        SELECT OrderID, ISBN FROM oltp.Sales_Q2
        UNION ALL
        SELECT OrderID, ISBN FROM oltp.Sales_Q3
        UNION ALL
        SELECT OrderID, ISBN FROM oltp.Sales_Q4
    ) AS sq ON e.ISBN = sq.ISBN;
"""

olap_cursor.execute(query_insert_book_fact)

# Commit the changes to the OLAP database
olap_cnx.commit()

# Close the cursors and connections
oltp_cursor.close()
oltp_cnx.close()
olap_cursor.close()
olap_cnx.close()