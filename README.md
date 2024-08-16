<<<<<<< HEAD
# CAPSTONE_PROJECT
=======
## Table of Contents

1. [Introduction](#introduction)
    <ul>
    <li>Overview of Data Engineering Importance</li>
    <li>Project Focus and Objectives</li>
    </ul>

2. [Project Overview](#project-overview)
    <ul>
    <li>End-to-End ELT Pipeline Development</li>
    <li>Project Steps Overview</li>
    </ul>
3. [Dataset](#dataset)
    <ul>
    <li>Description of the Kaggle Dataset</li>
    <li>Link to the Dataset</li>
    </ul>
4. [Data Schema](#data-schema)
    <ul>
    <li>Visual Representation of the Data Schema</li>
    </ul>
5. [Tools & Technologies Used](#tools--technologies-used)
    <ul>
    <li>List of Tools and Technologies</li>
    </ul>
6. [Data Architecture](#data-architecture)
    <ul>
    <li>Overview of Data Architecture</li>
    <li>Visual Representation of Data Flow</li>
    </ul>
7. [Project Flow](#project-flow)
    <ul>
    <li>Data Extraction</li>
    <li>Data Loading</li>
    <li>Data Transformation</li>
    <li>Workflow Automation and Orchestration</li>
    </ul>
8. [How to Run the Project](#how-to-run-the-project)
    <ul>
    <li>Setting Up the Environment</li>
    <li>Data Ingestion into PostgreSQL</li>
    <li>Setting Up Apache Airflow</li>
    <li>Loading Data into Google Cloud Storage</li>
    <li>Loading Data into BigQuery</li>
    <li>Transforming Data with DBT</li>
    <li>Answering Analytical Questions</li>
    <li>Automating and Orchestrating the Workflow</li>
    <li>Monitoring and Troubleshooting</li>
    </ul>
9. [Explanation of Each Model and Transformation in dbt](#explanation-of-each-model-and-transformation-in-dbt)
    <ul>
    <li>Staging Models</li>
    <li>Intermediate Models</li>
    <li>Final Models</li>
    </ul>
10. [Summary](#summary)
    <ul>
    <li>Recap of Project Structure and Transformation Process</li>
    </ul>



# Introduction
In today’s data-centric environment, the ability to manage and utilize vast amounts of data is crucial for making informed business decisions. Data engineers are essential in this process, tasked with centralizing data, ensuring its accuracy and relevance, and making it accessible for analysis. This project focuses on building a comprehensive ELT pipeline that streamlines these tasks, using a real-world dataset to address specific analytical questions.

# Project Overview
In this project, an end to end ELT pipeline is developed using a dataset from Kaggle to help data end users answer some analytical questions. 

## Project Steps
1. Data Ingestion into PostgreSQL
2. Setting up Apache Airflow
3. Loading Data from PostgreSQL to GCS and then to BigQuery
4. Transforming and Modeling Data with dbt
5. Answering Analytical Questions


# Dataset
This data used in this project was obtained from Kaggle [dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/).
This is a Brazilian ecommerce public dataset of orders made at Olist Store. The dataset has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil.

# Data Schema
<p align="center">
  <img src="/Users/mac/Desktop/capstone_project/Setup/images/image1.png" alt="Data_schema" width="600"/>
</p>


# Tools & Technologies used:
<ol>
<li> Cloud: Google Cloud Platform (GCP)</li>
<li> Containerization: Docker, Docker Compose</li>
<li> Workflow Orchestration: Apache Airflow</li>
<li> Data Lake: Google Cloud Storage (GCS)</li>
<li> Data Warehouse: Big Query</li>
<li> Data Transformation: Data Build Tool (DBT)</li>
<li> Programming Language: Python (batch processing), SQL (data transformation)</li>
</ol>

# Data Architecture
<p align="center">
  <img src="/Users/mac/Desktop/capstone_project/Setup/images/data_architecture.gif" alt="Pipeline Diagram" width="600"/>
</p>

# Project Flow:

1. Data Extraction: Data is retrieved from postgres and then loaded into Google Cloud Storage.
2. Data Loading: The converted data is stored in Google Cloud Storage, the data lake, and then loaded into BigQuery, the data warehouse.
3. Data Transformation: DBT is connected to BigQuery to transform the raw data, after which the processed data is loaded back into BigQuery; with the entire ELT process automated and orchestrated using Apache Airflow

# How to run the project

Below are steps on how to run this pipeline;


1. Set Up Your Environment:
<ul>
<li>Ensure that Docker and Docker Compose are installed on your machine. Docker will be used to containerize and run your services.</li>
</ul>

2. Ingest Data into PostgreSQL:
<ul>
<li>Create a schema in the init.sql file</li>
<li>Start the PostgreSQL container using Docker Compose with the command</li>
<li>Once PostgreSQL is running, ingest the data from the CSV files into the PostgreSQL database.</li>
</ul>

3. Set Up Apache Airflow:
<ul>
<li>Start the Airflow service using Docker Compose</li>
<li>Access the Airflow web UI (typically at http://localhost:8080), and trigger the DAG (Directed Acyclic Graph) responsible for orchestrating the ETL pipeline.</li>
<li>create your dags to extract data from PostgreSQL and store it in Google Cloud Storage and load data from Google Cloud Storage to Google BigQuery</li>
</ul>

<p align="center">
  <img src="/Users/mac/Desktop/capstone_project/Setup/images/airflow.jpeg" alt="Airflow" width="600"/>
</p>

4. Load Data from PostgreSQL to Google Cloud Storage:
<ul>
<li>Within the Airflow DAG, the first task extracts data from PostgreSQL and uploads it to a bucket in Google Cloud Storage (GCS).
<li>Ensure that your GCP credentials are correctly set up to allow access to GCS.
</ul>

5. Load Data into BigQuery:
<ul>
<li>The second task in the DAG loads the data from GCS into Google BigQuery. Ensure your BigQuery dataset is properly configured to receive this data.</li>
</ul>

6. Transform Data Using DBT:
<ul>
<li>Install DBT and set up the connection to your BigQuery dataset. Follow the instructions in the DBT documentation or the provided dbt_project.yml file in this repository.</li>
<li>Run DBT models to transform the raw data into the desired format</li>
</ul>

<p align="center">
  <img src="/Users/mac/Desktop/capstone_project/Setup/images/bigquery.png" alt="Bigquery" width="600"/>
</p>

7. Answer Analytical Questions:
<ul>
<li>Once the data is transformed, you can query the processed data in BigQuery to answer the analytical questions defined in your project scope.</li>
</ul>

8. Automate and Orchestrate the Workflow:
<ul>
<li>The entire pipeline, from data ingestion to transformation, can be automated and orchestrated using the Airflow DAG. Make sure all tasks are properly linked and scheduled within Airflow.</li>
</ul>

9. Monitor and Troubleshoot:
<ul>
<li>Monitor the pipeline through the Airflow web UI. If any task fails, use the logs and retry mechanisms in Airflow to debug and resolve the issue.</li>
</ul>



# Explanation of each model and transformation in dbt.

This is a typical structure for a dbt (data build tool) project, where you organize your models into Staging, Intermediate, and Final layers. 
Here's an explanation of each section and transformation:

## Staging Models
Staging models are typically used to bring in raw data from your data sources and prepare them for further transformations. These models generally select all columns from the source tables without making any transformations.

1. select * from {{ source('dataset', 'customer') }}
This selects all columns from the customer table in your raw dataset.

2. select * from {{ source('dataset', 'geolocation') }}
This selects all columns from the geolocation table in your raw dataset.

3. select * from {{ source('dataset', 'order_items') }}
This selects all columns from the order_items table in your raw dataset.

4. select * from {{ source('dataset', 'order_payments') }}
This selects all columns from the order_payments table in your raw dataset.

5. select * from {{ source('dataset', 'order_reviews') }}
This selects all columns from the order_reviews table in your raw dataset.

6. select * from {{ source('dataset', 'orders') }}
This selects all columns from the orders table in your raw dataset.

7. select * from {{ source('dataset', 'product_category_name_translation') }}
This selects all columns from the product_category_name_translation table in your raw dataset.

8. select * from {{ source('dataset', 'products') }}
This selects all columns from the products table in your raw dataset.

9. select * from {{ source('dataset', 'sellers') }}
This selects all columns from the sellers table in your raw dataset.

## Intermediate Models
Intermediate models perform transformations that typically involve joining tables, filtering data, and aggregating information. These models create more meaningful datasets that can be used in the final models.

1. Sales by Product Category (int_sales_by_category):
Purpose: Calculate the total sales for each product category.
Transformation:
The sales CTE (Common Table Expression) joins the orders, order_items, and products tables to get order_id, product_category_name, and price.
The final query then aggregates the sales data by product_category_name using SUM(price).

2. Average Delivery Time (int_avg_delivery_time):
Purpose: Calculate the average delivery time for delivered orders.
Transformation:
The avg_delivery CTE calculates the difference in hours between order_delivered_customer_date and order_purchase_timestamp for each order.
The final query returns this information for further aggregation or use.

3. Orders by State (int_orders_by_state):
Purpose: Count the number of delivered orders by customer state.
Transformation:
The orders_by_state CTE joins the orders and customers tables and counts the number of orders for each state.
The final query returns the count of orders by state, ordered alphabetically by state name.

## Final Models
Final models typically involve simple queries that produce the final results to be used for reporting or analysis. They often reference the intermediate models.

1. Top Sales Category:
Purpose: Identify the product category with the highest total sales.
Transformation:
This query selects all columns from the int_sales_by_category model, orders the results by total_sales in descending order, and limits the results to the top 1.

2. Overall Average Delivery Time:
Purpose: Calculate the overall average delivery time across all orders.
Transformation:
This query selects the average of avg_delivery_time_hour from the int_avg_delivery_time model, giving you the total average delivery time.

3. Top State by Order Count:
Purpose: Identify the state with the highest number of delivered orders.
Transformation:
This query selects all columns from the int_orders_by_state model, orders the results by order_count in descending order, and limits the results to the top 1.

### Summary
Staging Models bring raw data into dbt without transformations.
Intermediate Models perform more complex transformations like joins, aggregations, and filtering.
Final Models produce the final outputs, often used for reporting or further analysis.
This structure ensures that the transformations are modular, maintainable, and easy to understand.
>>>>>>> 44064ca (capstone_project)
