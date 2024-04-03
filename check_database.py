import psycopg2 as pg


#connect to the server

conn = pg.connect(
    dbname='your_warehouse_db_name',  
    user = 'your_user',
    password = 'your_password',
    host = 'your_host',
    port = 'your_port' #default port is '5432
)
cursor = conn.cursor()

# retrieve data from the table
cursor.execute('''
    SELECT * FROM aggregated_sales;
''')

# print the data
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()