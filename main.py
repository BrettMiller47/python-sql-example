import mysql.connector
from mysql.connector import Error
import pandas as pd
from env import username, password


def create_server_connection(host_name, user_name, user_password):
    """
        Creates a MySQL connection to the SQL database.
        :param host_name: server's host name (typically, localhost)
        :param user_name: username for the SQL connection
        :param user_password: password for the SQL connection
        :return:
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print('MySQL Database connection successful')
    except Error as err:
        print(f'Error: {err}')
    return connection


# Variable for the yet-to-be-created database's name
db = 'mysql_python'
# Variable connection to the server
connection = create_server_connection('localhost', username, password)


def create_database(connection, query):
    """
    Creates a SQL database.
    :param connection: server connection
    :param query: MySQL query to create a database
    :return:
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print(f'Database created successfully')
    except Error as err:
        print(f'Error: {err}')


# Create the database
create_database_query = f"Create database {db}"
create_database(connection, create_database_query)


def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Creates a connection to a specific SQL database.
    :param host_name: server's host name (typically, localhost)
    :param user_name: username for the SQL connection
    :param user_password: password for the SQL connection
    :param db_name: the SQL database name
    :return:
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print(f'MySQL database connection successful')
    except Error as err:
        print(f'Error: {err}')
    return connection


# Execute sql queries
def execute_query(connection, query):
    """
    Executes a MySQL query.
    :param connection: SQL database connection
    :param query: MySQL query
    :return:
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print(f'Query was successful')
    except Error as err:
        print(f'Error: {err}')


# SQL script to create a table named 'orders'
create_orders_table = """
create table orders(
    order_id int primary key,
    customer_name varchar(30) not null,
    product_name varchar(20) not null,
    date_ordered date,
    quantity int,
    unit_price float,
    cust_phone_number varchar(20)
);
"""

# Connect to the database
connection = create_db_connection('localhost', username, password, db)
# Create the 'orders' table
execute_query(connection, create_orders_table)

# SQL script to create a table named 'orders'
insert_data_orders = """
insert into orders values
(101, 'Alice', 'Burger', '2023-01-01', 2, 11.99, '000111222'),
(102, 'Bob', 'Fries', '2023-01-01', 2, 4.99, '333444555'),
(103, 'Carol', 'Burger', '2023-01-01', 2, 11.99, '000111222'),
(104, 'Dave', 'Fries', '2023-01-01', 2, 4.99, '333444555'),
(105, 'Erin', 'Soda', '2023-01-01', 2, 2.49, '666777888');
"""

# Insert data into 'orders' table
connection = create_db_connection('localhost', username, password, db)
execute_query(connection, insert_data_orders)


def read_query(connection, query):
    """
    Runs a query on the SQL database.
    :param connection: connection to the SQL database
    :param query: SQL query
    :return: query results
    """
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f'ErrorL {err}')


# SQL statement to select all orders
select_all_orders_query = """
select * from orders;
"""

# Print all orders as a dataframe
connection = create_db_connection('localhost', username, password, db)
select_all_results = read_query(connection, select_all_orders_query)
results_from_db = []
for result in select_all_results:
    result = list(result)
    results_from_db.append(result)
columns = ['order_id',
           'customer_name',
           'product_name',
           'date_ordered',
           'quantity',
           'unit_price',
           'cust_phone_number'
]
df = pd.DataFrame(results_from_db, columns=columns)
print(df)

# SQL statement to select all orders where the item is priced greater than $5
select_items_ordered_greater_than_five_query = """
select distinct product_name, unit_price from orders
where unit_price > 5;
"""

# Print all orders
connection = create_db_connection('localhost', username, password, db)
select_items_ordered_greater_than_five_results = read_query(connection, select_items_ordered_greater_than_five_query)
for result in select_items_ordered_greater_than_five_results:
    print(result)

# SQL command to update the price of fries to $6
update_fries_price_query = """
update orders
set unit_price = 6
where product_name = 'Fries'
"""

# Update the price of fries to $6 and print results as dataframe
connection = create_db_connection('localhost', username, password, db)
execute_query(connection, update_fries_price_query)
select_all_results = read_query(connection, select_all_orders_query)
results_from_db = []
for result in select_all_results:
    result = list(result)
    results_from_db.append(result)
columns = ['order_id',
           'customer_name',
           'product_name',
           'date_ordered',
           'quantity',
           'unit_price',
           'cust_phone_number'
]
df = pd.DataFrame(results_from_db, columns=columns)
print(df)

# SQL command to delete an order by order_id
delete_order = """
delete from orders
where order_id = 103
"""

# Delete order 103 and print results as dataframe
connection = create_db_connection('localhost', username, password, db)
execute_query(connection, delete_order)
select_all_results = read_query(connection, select_all_orders_query)
results_from_db = []
for result in select_all_results:
    result = list(result)
    results_from_db.append(result)
columns = ['order_id',
           'customer_name',
           'product_name',
           'date_ordered',
           'quantity',
           'unit_price',
           'cust_phone_number'
]
df = pd.DataFrame(results_from_db, columns=columns)
print(df)
