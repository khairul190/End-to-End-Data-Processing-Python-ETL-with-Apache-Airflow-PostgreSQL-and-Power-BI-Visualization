# End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization
I developed an ETL process using Python (Apache Airflow) with PostgreSQL, followed by crafting visualizations using Power BI. Streamlining data extraction, transformation, and loading for efficient analysis and reporting.


# WORKFLOW
![image](https://github.com/khairul190/End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization/assets/57305430/bfe56a94-bce2-443e-b5ab-cadefb9db5a8)









# Airflow Pipeline
![image](https://github.com/khairul190/End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization/assets/57305430/d8ac4da7-3acd-4cc3-a151-ff55ebb5285b)

- Saya menggunakan sql sensor untuk mengecek apalah data yang di ingest ke staging telah tersedia sebelum melakukan ingest ke DWH


# Data Mapping Detail ( Staging to dwh )
![image](https://github.com/khairul190/End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization/assets/57305430/18b8333b-e610-4798-b950-dd497afee0fe)
## Mapping 5 Tables (not error) using sql
![image](https://github.com/khairul190/End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization/assets/57305430/872ccc7c-11e0-45b2-a1a9-af82d1f35077)

mapping pada 5 table dwh yaitu pada table orders, customer, product, region_mgr, location

## Mapping error log Table using sql
![image](https://github.com/khairul190/End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization/assets/57305430/77b5a49b-412b-4729-a37c-112a350ca7a9)



# Power BI Data Visualization
![image](https://github.com/khairul190/End-to-End-Data-Processing-Python-ETL-with-Apache-Airflow-PostgreSQL-and-Power-BI-Visualization/assets/57305430/402398c7-b326-4295-bd27-6025dc418577)
