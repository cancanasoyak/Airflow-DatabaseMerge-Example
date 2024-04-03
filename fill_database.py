import psycopg2 as pg


#connect to the server

conn = pg.connect(
    dbname='your_db_name',  
    user = 'your_user',
    password = 'your_password',
    host = 'your_host',
    port = 'your_port' #default port is '5432
)
cursor = conn.cursor()

# create a new table if it does not exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS online_sales (
        sale_id SERIAL PRIMARY KEY,
        product_id INT,
        quantity INT,
        sale_amount DECIMAL(10, 2),
        sale_date DATE
    );
''')

# insert data into the table
cursor.execute('''
    INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date)
    VALUES
    (101, 2, 40.00, '2024-03-01'),
    (102, 1, 20.00, '2024-03-01'),
    (103, 3, 60.00, '2024-03-02'),
    (104, 1, 20.00, '2024-03-02');
    '''
)
conn.commit()

# retrieve data from the table
cursor.execute('''
    SELECT * FROM online_sales;
''')

# print the data
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()