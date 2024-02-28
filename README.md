# DATA ENGINEER >><< >> Project Audiobook shop
this project is the project in course **"Road To Data Engineer 2.0"** by **"Data TH"** 

## ***introduction ***
---------------
The objective of this project is to collect, integrate, transform, and clean data for Data Analyst **as Data Engineer** in the Audiobook shop company. In addition, use the completed data to create a dashboard following the business requirement that needs to know the best selling product for selecting the product to promote and preparing appropriate promotions to increase audiobook sales

## ***Folders and Files explaination***
---------------------
**In *Github***
- **dags** : store DAG script
    - audiobook_dag.py : DAG Python script for data pipeline by using Apache Airflow
**In *Google drive*** : following this link ---> [data in this project]
- **data** : store data in this project
    - **input**
        - audible_data.csv : Audiobook data
        - conversion_rate.csv : Conversion rate data for convert US Dollar to Thai Baht
        - audible_transaction_data.csv : Transaction sales data
        - audible_data_merged.csv : Data from merge between audible_data.csv and audible_transaction_data.csv 
    - **output**
        - output.csv : Data from merge between audible_data_merged.csv and conversion_rate.csv along with data transformation
## ***Methodology***
--------------
0. Plan to manipulate the project using colab notebook
    - Explore data in database(MySQL in this project), in database has 2 tables : audible_data and audible_transaction.
    - Then fetch conversion rate data from REST API 
    - Mock the Transformation process using PANDAS and Data Cleaning using PySpark (Python API of Apache Spark) in colab notebook
1. Create **Cloud Composer Cluster** for running **Apache Airflow** and install python package in cluster(pymysql, requests, pandas)
2. **[ Credential!!! ]** Set MySQL connection on Apache Aiflow web server (Admin-->Connection-->mysql_default)
4. Manipulate and Create folders as below:
    - In the auto-generated Cloud Storage when we create Cloud Composer Cluster
        - Create folder **input** and **output** in folder **data** for input and output data in this project
5. Upload pipeline python script : *audiobook.py* to folder **dags**(connected to dag folder in Airflow) by using Cloud shell
6. Airflow implements Data Pipeline Orchestration and triggers following a schedule that config in *audiobook.py*. DAG of this pipeline is shown below:

    ![DAG_pic]
7. Create a view table from a table in Dataset in **BigQuery**. Then use this view table to make Sales Dashboard in **Looker Studio**
8. See the Sales Dashboard as the attached link below:  
    [https://lookerstudio.google.com/reporting/88ed731e-4c2b-4b99-82f7-da0dbc1172de]

## ***Technologies Used***
----------------------
- MySQL
- Google Cloud Platform
    - Cloud Storage
    - Cloud Composer
    - BigQuery
- Looker Studio
- Apache Airflow
- Apache Spark
## Languages
-------------
- Python
- SQL
- Spark
