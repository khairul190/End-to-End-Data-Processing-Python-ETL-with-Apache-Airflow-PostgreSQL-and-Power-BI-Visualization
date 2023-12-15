from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy.exc import IntegrityError
from airflow.sensors.sql_sensor import SqlSensor
import xml.etree.ElementTree as ET
from openpyxl import Workbook
import connection
import pendulum
from datetime import datetime, timedelta

db_connection=connection.postgresql_con()
log_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
local_tz = pendulum.timezone("Asia/Jakarta")
email_receiver = [
    "muhkhairul190@gmail.com"
]


args = {
    'owner': 'khairul',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 14, tzinfo=local_tz),
    'email': email_receiver,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_superstore',
    default_args=args,
    description='etl superstore',
    schedule_interval='2 17 * * *', 
    concurrency=2
) as dag :

    start = DummyOperator(task_id="start_process", dag=dag)
    end = DummyOperator(task_id="end_process", dag=dag)
    wait = DummyOperator(task_id="wait_process", trigger_rule="all_success", dag=dag)
    wait_sensing = DummyOperator(task_id="wait_sensing", trigger_rule="all_success", dag=dag)

#----------------------Staging-------------------------

    def import_excel_to_staging():
        read_excel = pd.read_excel('file_path')
        read_excel['Order_Date'] = read_excel['Order_Date'].dt.strftime('%Y-%m-%d')
        read_excel['Ship_Date'] = read_excel['Ship_Date'].dt.strftime('%Y-%m-%d')
        table_name='superstore'
        schema_name='staging'
        read_excel.to_sql(table_name, con=db_connection, if_exists='append', index=False, schema=schema_name)
        print(f"Data berhasil masuk ke tabel {table_name} di PostgreSQL.")

    def import_json_to_staging():
        data_json = pd.read_json('file_path')
        table_name='people'
        schema_name='staging'
        data_json.to_sql(table_name, con=db_connection, if_exists='append', index=False, schema=schema_name)
        print(f"Data berhasil masuk ke tabel {table_name} di PostgreSQL.")

    def json_to_staging():
        file_path = r'file_path'
        tree = ET.parse(file_path)
        root = tree.getroot()

        ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}
        order_ids = []
        returned_values = []

        for row in root.findall('.//ss:Worksheet[@ss:Name="Returns"]//ss:Row', namespaces=ns):
            cells = row.findall('ss:Cell', namespaces=ns)
            
            order_id = cells[0].find('ss:Data', namespaces=ns).text
            returned = cells[1].find('ss:Data', namespaces=ns).text

            order_ids.append(order_id)
            returned_values.append(returned)

        data = {'Order_ID': order_ids, 'Returned': returned_values}
        df = pd.DataFrame(data)
        df = df.drop(index=0)
        
        table_name='returns'
        schema_name='staging'
        df.to_sql(table_name, con=db_connection, if_exists='append', index=False, schema=schema_name)
        print(f"Data berhasil masuk ke tabel {table_name} di PostgreSQL.")

#--------------------------DWH-------------------

    def stg_to_region_mgr():
        proc_name="SYN_REGION_MGR"
        with db_connection.connect() as connection:
            select_query = connection.execute("SELECT \"Person\", \"Region\" FROM \"staging\".people")
            insert_query = "INSERT INTO dwh.region_mgr (PERSON, REGION) VALUES (%s, %s)"

            for row in select_query.fetchall():
                data = (row["Person"], row["Region"])
                
                try:
                    connection.execute(insert_query, data)
                except IntegrityError as e:
                    # print(f"Region={row['Region']} : unique constraint (xpkregion_mgr) violated")
                    err_msg=f"Region={row['Region']} : unique constraint (xpkregion_mgr) violated"
                    insert_log_query = "INSERT INTO dwh.err_log (PROC_NAME, LOG_DATE, ERR_MSG) VALUES (%s, %s, %s)"
                    connection.execute(insert_log_query, (proc_name, log_date, err_msg))
        print(f"Data berhasil ingest dari stg.people ke dwh.region_mgr di PostgreSQL.")

    def stg_to_customer():
        proc_name="SYN_CUSTOMER"
        with db_connection.connect() as connection:
            select_query = connection.execute("SELECT \"Customer_ID\", \"Customer_Name\", \"Segment\" FROM \"staging\".superstore")
            insert_query = "INSERT INTO dwh.customer (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT) VALUES (%s, %s, %s)"

            for row in select_query.fetchall():
                data = (row["Customer_ID"], row["Customer_Name"], row["Segment"])
                
                try:
                    connection.execute(insert_query, data)
                except IntegrityError as e:
                    # print(f"Customer_ID={row['Customer_ID']} : unique constraint (customer_pkey) violated")
                    err_msg=f"Customer_ID={row['Customer_ID']} : unique constraint (customer_pkey) violated"
                    insert_log_query = "INSERT INTO dwh.err_log (PROC_NAME, LOG_DATE, ERR_MSG) VALUES (%s, %s, %s)"
                    connection.execute(insert_log_query, (proc_name, log_date, err_msg))
        print(f"Data berhasil ingest dari stg.superstore ke dwh.customer di PostgreSQL.")

    def stg_to_location():
        proc_name="SYN_LOCATION"
        with db_connection.connect() as connection:
            select_query = connection.execute("SELECT \"Postal_Code\", \"Country\", \"Region\", \"State\", \"City\" FROM \"staging\".superstore")
            insert_query = "INSERT INTO dwh.location (ZIPCODE, COUNTRY, REGION, STATE, CITY) VALUES (%s, %s, %s, %s, %s)"

            for row in select_query.fetchall():
                data = (row["Postal_Code"], row["Country"], row["Region"], row["State"], row["City"])
                
                try:
                    connection.execute(insert_query, data)
                except IntegrityError as e:
                    # print({e})
                    # print(f"ZIPCODE={row['Postal_Code']} : unique constraint (idx_location_zipcode) violated")
                    err_msg=f"ZIPCODE={row['Postal_Code']} : unique constraint (idx_location_zipcode) violated"
                    insert_log_query = "INSERT INTO dwh.err_log (PROC_NAME, LOG_DATE, ERR_MSG) VALUES (%s, %s, %s)"
                    connection.execute(insert_log_query, (proc_name, log_date, err_msg))
        print(f"Data berhasil ingest dari stg.superstore ke dwh.location di PostgreSQL.")

    def stg_to_product():
        proc_name="SYN_PRODUCT"
        with db_connection.connect() as connection:
            select_query = connection.execute("SELECT \"Product_ID\", \"Category\", \"Sub_Category\", \"Product_Name\" FROM \"staging\".superstore")
            insert_query = "INSERT INTO dwh.product (PRODUCT_ID, CATEGORY, SUB_CATEGORY, PRODUCT_NAME) VALUES (%s, %s, %s, %s)"

            for row in select_query.fetchall():
                data = (row["Product_ID"], row["Category"], row["Sub_Category"], row["Product_Name"])
                
                try:
                    connection.execute(insert_query, data)
                except IntegrityError as e:
                    # print({e})
                    # print(f"PRODUCT_ID={row['Product_ID']} : unique constraint (product_pkey) violated")
                    err_msg=f"PRODUCT_ID={row['Product_ID']} : unique constraint (product_pkey) violated"
                    insert_log_query = "INSERT INTO dwh.err_log (PROC_NAME, LOG_DATE, ERR_MSG) VALUES (%s, %s, %s)"
                    connection.execute(insert_log_query, (proc_name, log_date, err_msg))
        print(f"Data berhasil ingest dari stg.superstore ke dwh.product di PostgreSQL.")

    def stg_to_orders():
        proc_name="SYN_ORDERS"
        with db_connection.connect() as connection:
            select_query = connection.execute("SELECT \"Order_Date\", \"Order_ID\", \"Product_ID\", \"Customer_ID\", \"Ship_Date\", \"Postal_Code\", \"Sales\", \"Quantity\", \"Discount\", \"Profit\" FROM \"staging\".superstore")
            insert_query = "INSERT INTO dwh.orders (ORDER_DATE, ORDER_ID, PRODUCT_ID, CUSTOMER_ID, SHIP_DATE, ZIPCODE, SALES, QUANTITY, DISCOUNT, PROFIT, RETURNED) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'No')"

            for row in select_query.fetchall():
                data = (    row["Order_Date"], row["Order_ID"], row["Product_ID"],
                            row["Customer_ID"], row["Ship_Date"], row["Postal_Code"],
                            row["Sales"], row["Quantity"], row["Discount"],
                            row["Profit"]
                        )
                try:
                    connection.execute(insert_query, data)
                except IntegrityError as e:
                    # print({e})
                    # print(f"ORDER_ID={row['Order_ID']} ORDER_DATE={row['Order_Date']} PRODUCT_ID={row['Product_ID']} SHIP_DATE={row['Ship_Date']} : unique constraint (TABLEAU.XPKORDERS) violated")
                    err_msg=f"ORDER_ID={row['Order_ID']} ORDER_DATE={row['Order_Date']} PRODUCT_ID={row['Product_ID']} SHIP_DATE={row['Ship_Date']} : unique constraint (TABLEAU.XPKORDERS) violated"
                    insert_log_query = "INSERT INTO dwh.err_log (PROC_NAME, LOG_DATE, ERR_MSG) VALUES (%s, %s, %s)"
                    connection.execute(insert_log_query, (proc_name, log_date, err_msg))    
            connection.execute("""
                    UPDATE dwh.orders
                    SET Returned = 'Yes'
                    FROM staging.returns
                    WHERE dwh.orders."order_id" = "staging"."returns"."Order_ID"
                    AND "staging"."returns"."Returned" = 'Yes';
                    """)
            print(f"Data berhasil ingest dari stg.superstore ke dwh.orders di PostgreSQL.")
            print(f"Data berhasil di update pada dwh.orders.returns berdasarkan yang melakukan returned.")

#-------------------------- SENSING --------------------
    with TaskGroup("SENSING_STAGING") as sensing_staging:
        sensing_staging_superstore = SqlSensor(
            task_id="sensing_staging_superstore",
            conn_id="postgres_local",
            sql=f"select count(1) from staging.superstore",
            dag=dag,
            mode='reschedule')
        
        wait >> sensing_staging_superstore >> wait_sensing

        sensing_staging_people = SqlSensor(
            task_id="sensing_staging_people",
            conn_id="postgres_local",
            sql=f"select count(1) from staging.people",
            dag=dag,
            mode='reschedule')
        
        wait >> sensing_staging_people >> wait_sensing

        sensing_staging_returns = SqlSensor(
            task_id="sensing_staging_returns",
            conn_id="postgres_local",
            sql=f"select count(1) from staging.returns",
            dag=dag,
            mode='reschedule')
        
        wait >> sensing_staging_returns >> wait_sensing

#--------------------------JOB---------------------

    with TaskGroup("STAGING") as staging:
        import_excel = PythonOperator(
            task_id='excel_to_staging.superstore',
            python_callable=import_excel_to_staging,
            dag=dag,
        )
        start >> import_excel >> wait 

        import_json = PythonOperator(
            task_id='json_to_staging.people',
            python_callable=import_json_to_staging,
            dag=dag,
        )
        start >> import_json >> wait 

        import_xml = PythonOperator(
            task_id='xml_to_staging.returns',
            python_callable=json_to_staging,
            dag=dag,
        )
        start >> import_xml >> wait 

    with TaskGroup("DWH") as dwh:
        tf_stg_to_customer = PythonOperator(
            task_id='stg_to_dwh.customer',
            python_callable=stg_to_customer,
            dag=dag,
        )
        
        tf_stg_to_location = PythonOperator(
            task_id='stg_to_dwh.location',
            python_callable=stg_to_location,
            dag=dag,
        )
    
        tf_stg_to_orders = PythonOperator(
            task_id='stg_to_dwh.orders',
            python_callable=stg_to_orders,
            dag=dag,
        )
       
        tf_stg_to_product = PythonOperator(
            task_id='stg_to_dwh.product',
            python_callable=stg_to_product,
            dag=dag,
        )

        tf_stg_to_region_mgr = PythonOperator(
            task_id='stg_to_dwh.region_mgr',
            python_callable=stg_to_region_mgr,
            dag=dag,
        )
        
wait_sensing >> tf_stg_to_customer >> tf_stg_to_region_mgr >> tf_stg_to_location >> tf_stg_to_product >> tf_stg_to_orders >> end







