import os
import pandas as pd
import psycopg2 as pg
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

salesdb_connstr = Variable.get("salesdb_connstr")
warehousedb_connstr = Variable.get("warehousedb_connstr")
os.makedirs("/data/tmp", exist_ok=True)


default_args = {
    "owner": "burak",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 3, 30),
    "schedule_interval": timedelta(days=1),
}

def fetch_csv_data():
    """
    Returns the address of the csv file for the store sales data.
    """
    csv_address = "/data/store_sales.csv" # the address of the csv file for the store sales data
    return csv_address


def fetch_pg_data(connstr):
    """
    Fetches the data from the online_sales table in the database 
    and saves it to a temporary csv file.
    """
    csv_file_name = "online_sales.csv" # the name of the temporary csv file
    
    conn = pg.connect(connstr)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM online_sales;
    ''')
    # save the data using pandas to a temporary file
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
    df.to_csv(f"/data/tmp/{csv_file_name}", index=False)
    cursor.close()
    conn.close()
    return f"/data/tmp/{csv_file_name}"
    
def structure_data(data_adress):
    """
    Reads the data from the csv file and structures it,
    seperating the rows with null values and
    saves the structured data to a temporary file.
    """
    csv_file_name = os.path.basename(data_adress)
    df = pd.read_csv(data_adress)
    # seperate the rows with null values
    df_null = df[df.isnull().any(axis=1)]
    df = df.dropna()
    
    # convert the data types
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["sale_amount"] = df["sale_amount"].astype(float)
    
    # save the structured data to a temporary file
    df.to_csv(f"/data/tmp/structured_{csv_file_name}", index=False)
    df_null.to_csv(f"/data/tmp/null_{csv_file_name}", index=False)
    
    return  f"/data/tmp/structured_{csv_file_name}",f"/data/tmp/null_{csv_file_name}"
    
    
def combine_data(**kwargs):
    ti = kwargs['ti']
    online_data_adress = ti.xcom_pull(task_ids='structure_pgdata')[0] # the address of the structured online data
    store_data_adress = ti.xcom_pull(task_ids='structure_storedata')[0] # the address of the structured store data

    df1 = pd.read_csv(online_data_adress)
    df2 = pd.read_csv(store_data_adress)
    # combine the data and Aggregate the data to calculate the total `quantity` and total `sale_amount` for each `product_id`.
    df = pd.concat([df1, df2])
    df = df.groupby("product_id").agg({"quantity": "sum", "sale_amount": "sum"}).sort_index()
    # save the combined data to a temporary file
    df.to_csv("/data/tmp/aggregated_data.csv")

def upload_to_pgwarehouse(connstr):
    """
    Uploads the aggregated data to the warehouse database.
    """
    conn = pg.connect(connstr)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS aggregated_sales (
            product_id INT PRIMARY KEY,
            total_quantity INT,
            total_sale_amount DECIMAL(10, 2)
        );
    ''')
    
    conn.commit()
    
    df = pd.read_csv("/data/tmp/aggregated_data.csv")
    for row in df.itertuples():
        cursor.execute('''
            INSERT INTO aggregated_sales (product_id, total_quantity, total_sale_amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE
            SET total_quantity = EXCLUDED.total_quantity, total_sale_amount = EXCLUDED.total_sale_amount;
        ''', (row.product_id, row.quantity, row.sale_amount))
    conn.commit()
    cursor.close()
    conn.close()

def clear_tmp():
    """
    Clears the temporary files.
    """
    try:
        for file in os.listdir("/data/tmp"):
            os.remove(f"/data/tmp/{file}")
    except:
        pass




dag = DAG(
    dag_id="main_task",
    description="Test for now",
    default_args=default_args,
)


start = EmptyOperator(
    task_id="start",
    dag=dag
)

# Fetch and Structure the Store Data
fetch_cvs_operator = PythonOperator(
    task_id="fetch_cvs",
    python_callable=fetch_csv_data,
    dag=dag
)
structure_storedata_operator = PythonOperator(
    task_id="structure_storedata",
    python_callable=structure_data,
    op_args=[fetch_cvs_operator.output],
    dag=dag
)

# Fetch and Structure the Online Data
fetch_pg_operator = PythonOperator(
    task_id="fetch_pg",
    python_callable=fetch_pg_data,
    op_args=[salesdb_connstr],
    dag=dag
)
structure_pgdata_operator = PythonOperator(
    task_id="structure_pgdata",
    python_callable=structure_data,
    op_args=[fetch_pg_operator.output],
    dag=dag
)

# Combine the Data
combine_operator = PythonOperator(
    task_id="combine_data",
    python_callable=combine_data,
    provide_context=True,
    dag=dag
)

upload_to_pgwarehouse_operator = PythonOperator(
    task_id="upload_to_pg",
    python_callable=upload_to_pgwarehouse,
    op_args=[warehousedb_connstr],
    dag=dag
)

clear_tmp_operator = PythonOperator(
    task_id="clear_tmp",
    python_callable=clear_tmp,
    dag=dag
)

end = EmptyOperator(
    task_id="end",
    dag=dag
)


start >> [fetch_cvs_operator, fetch_pg_operator]

fetch_cvs_operator >> structure_storedata_operator
fetch_pg_operator >> structure_pgdata_operator

[structure_storedata_operator, structure_pgdata_operator] >> combine_operator

combine_operator >> upload_to_pgwarehouse_operator

upload_to_pgwarehouse_operator >> clear_tmp_operator

clear_tmp_operator >> end



