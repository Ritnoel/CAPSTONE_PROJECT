
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime

GCS_BUCKET = 'my-capstone-bucket'
POSTGRES_CONNECTION_ID = 'db_conn'
GCS_CONN_ID = 'gcp_conn_id'
PG_SCHEMA = 'ECOMMERCE'

BIGQUERY_DATASET = 'dataset'
BIGQUERY_PROJECT = 'tenacious-coder-416410'

with DAG(
     dag_id='postgres_to_bigquery',
     start_date=datetime(2024, 7, 1),
     schedule_interval= None,
     catchup=False
) as dag:

#START TASK
    starttask = DummyOperator(
        task_id = 'starttask',
        dag = dag
    )

#CUSTOMERS TABLE
# Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_customers_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_customers_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_customers',
        bucket=GCS_BUCKET,
        filename='olist_customers.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )


    schema = [
        {
            'name': 'customer_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'customer_unique_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'customer_zip_code_prefix',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'customer_city',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'customer_state',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_customers_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_customers_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_customers.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.customer',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )

    #Cleanup_dataset
    cleanup_customers = GCSDeleteObjectsOperator(
        task_id='cleanup_customers',
        bucket_name=GCS_BUCKET,
        objects=['olist_customers.csv'],
        gcp_conn_id=GCS_CONN_ID, 
        dag=dag
    )

    
    #GEOLOCATION TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_geolocation_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_geolocation_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_geolocation',
        bucket=GCS_BUCKET,
        filename='olist_geolocation.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )

    

    schema = [
        {
            'name': 'geolocation_zip_code_prefix',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'geolocation_lat',
            'type': 'FLOAT64',
            'mode': 'NULLABLE',
        },
        {
            'name': 'geolocation_lng',
            'type': 'FLOAT64',
            'mode': 'NULLABLE',
        },
        {
            'name': 'geolocation_city',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'geolocation_state',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_geolocation_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_geolocation_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_geolocation.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.geolocation',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_geolocation = GCSDeleteObjectsOperator(
        task_id='cleanup_geolocation',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['olist_geolocation.csv'],
        dag=dag
    )


    #ORDER_ITEMS TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_order_items_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_order_items_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_order_items',
        bucket=GCS_BUCKET,
        filename='olist_order_items.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )

    

    schema = [
        {
            'name': 'order_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_item_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'seller_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'shipping_limit_date',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },
        {
            'name': 'price',
            'type': 'FLOAT64',
            'mode': 'NULLABLE',
        },
        {
            'name': 'freight_value',
            'type': 'FLOAT64',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_order_items_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_order_items_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_order_items.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.order_items',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_order_items = GCSDeleteObjectsOperator(
        task_id='cleanup_order_items',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['olist_order_items.csv'],
        dag=dag
    )


    
    #ORDER PAYMENTS TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_order_payments_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_order_payments_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_order_payments',
        bucket=GCS_BUCKET,
        filename='olist_order_payments.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )



    schema = [
        {
            'name': 'order_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'payment_sequential',
            'type': 'INT64',
            'mode': 'NULLABLE',
        },
        {
            'name': 'payment_type',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'payment_installments',
            'type': 'INT64',
            'mode': 'NULLABLE',
        },
        {
            'name': 'payment_value',
            'type': 'FLOAT64',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_order_payments_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_order_payments_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_order_payments.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.order_payments',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_order_payments = GCSDeleteObjectsOperator(
        task_id='cleanup_order_payments',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['olist_order_payments.csv'],
        dag=dag
    )


    
    # ORDER REVIEWS TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_order_reviews_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_order_reviews_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_order_reviews',
        bucket=GCS_BUCKET,
        filename='olist_order_reviews.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )

    

    schema = [
        {
            'name': 'review_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'review_score',
            'type': 'INTEGER',
            'mode': 'NULLABLE',
        },
        {
            'name': 'review_comment_title',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'review_comment_message',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
         {
            'name': 'review_creation_date',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },
        {
            'name': 'review_answer_timestamp',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_order_reviews_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_order_reviews_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_order_reviews.csv'],
        quote_character = '"',
        allow_quoted_newlines=True,
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.order_reviews',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_order_reviews = GCSDeleteObjectsOperator(
        task_id='cleanup_order_reviews',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['olist_order_reviews.csv'],
        dag=dag
    )


    
    #ORDERS TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_orders_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_orders_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_orders',
        bucket=GCS_BUCKET,
        filename='olist_orders.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )



    schema = [
        {
            'name': 'order_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'customer_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_status',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_purchase_timestamp',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_approved_at',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_delivered_carrier_date',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_delivered_customer_date',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },
        {
            'name': 'order_estimated_delivery_date',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_orders_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_orders_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_orders.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.orders',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


     #Cleanup_dataset
    cleanup_orders = GCSDeleteObjectsOperator(
        task_id='cleanup_orders',
        gcp_conn_id=GCS_CONN_ID, 
        bucket_name=GCS_BUCKET,
        objects=['olist_orders.csv'],
        dag=dag
    )


    
    #PRODUCTS TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_products_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_products_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_products',
        bucket=GCS_BUCKET,
        filename='olist_products.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )


    schema = [
        {
            'name': 'product_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_category_name',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_name_lenght',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_description_lenght',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_photos_qty',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_weight_g',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_length_cm',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_height_cm',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_width_cm',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_products_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_products_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_products.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.products',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_products = GCSDeleteObjectsOperator(
        task_id='cleanup_products',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['olist_products.csv'],
        dag=dag
    )


    #SELLERS
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_sellers_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_sellers_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.olist_sellers',
        bucket=GCS_BUCKET,
        filename='olist_sellers.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )

   

    schema = [
        {
            'name': 'seller_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'seller_zip_code_prefix',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'seller_city',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'seller_state',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_sellers_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_sellers_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['olist_sellers.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.sellers',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_sellers = GCSDeleteObjectsOperator(
        task_id='cleanup_sellers',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['olist_sellers.csv'],
        dag=dag
    )


    #PRODUCT CATEGORY NAME TRANSLATION TABLE
    # Task to extract data from PostgreSQL and store it in Google Cloud Storage    
    extract_product_category_name_translation_table_to_gcs = PostgresToGCSOperator(
        task_id='extract_product_category_name_translation_table_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        gcp_conn_id = GCS_CONN_ID,
        sql=f'SELECT * FROM {PG_SCHEMA}.product_category_name_translation',
        bucket=GCS_BUCKET,
        filename='product_category_name_translation.csv',
        export_format='csv',
        field_delimiter=',',
        gzip=False,
        dag=dag
    )

   

    schema = [
        {
            'name': 'product_category_name',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        {
            'name': 'product_category_name_english',
            'type': 'STRING',
            'mode': 'NULLABLE',
        },
        

    ]

    #Task to load data from Google Cloud Storage to Google BigQuery
    load_product_category_name_translation_table_to_bigquery = GCSToBigQueryOperator(
        task_id='load_product_category_name_translation_table_to_bigquery',
        bucket=GCS_BUCKET,
        gcp_conn_id = GCS_CONN_ID,
        source_objects=['product_category_name_translation.csv'],
        destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.product_category_name_translation',
        schema_fields = schema,
        source_format='CSV',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag
    )


    #Cleanup_dataset
    cleanup_product_category_name_translation = GCSDeleteObjectsOperator(
        task_id='cleanup_product_category_name_translation',
        bucket_name=GCS_BUCKET,
        gcp_conn_id=GCS_CONN_ID, 
        objects=['product_category_name_translation.csv'],
        dag=dag
    )

    #END TASK
    endtask = DummyOperator(
        task_id = 'endtask',
        dag = dag
    )

    
    starttask >> extract_customers_table_to_gcs >> load_customers_table_to_bigquery >> cleanup_customers
    cleanup_customers >> extract_geolocation_table_to_gcs >> load_geolocation_table_to_bigquery >> cleanup_geolocation
    cleanup_geolocation >> extract_order_items_table_to_gcs >> load_order_items_table_to_bigquery >> cleanup_order_items
    cleanup_order_items >> extract_order_payments_table_to_gcs >> load_order_payments_table_to_bigquery >> cleanup_order_payments
    cleanup_order_payments >> extract_order_reviews_table_to_gcs >> load_order_reviews_table_to_bigquery >> cleanup_order_reviews
    cleanup_order_reviews >> extract_orders_table_to_gcs >> load_orders_table_to_bigquery >> cleanup_orders
    cleanup_orders >> extract_products_table_to_gcs >> load_products_table_to_bigquery >> cleanup_products
    cleanup_products >> extract_sellers_table_to_gcs >> load_sellers_table_to_bigquery >> cleanup_sellers
    cleanup_sellers >> extract_product_category_name_translation_table_to_gcs >> load_product_category_name_translation_table_to_bigquery >> cleanup_product_category_name_translation >> endtask
    

    