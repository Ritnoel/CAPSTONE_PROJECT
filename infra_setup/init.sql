CREATE SCHEMA IF NOT EXISTS ECOMMERCE;

create table if not exists ECOMMERCE.olist_customers
(
    customer_id varchar,
    customer_unique_id varchar,
    customer_zip_code_prefix varchar,
    customer_city varchar,
    customer_state varchar
);
COPY ECOMMERCE.olist_customers(customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state)
FROM '/data/olist_customers.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_geolocation
(
    geolocation_zip_code_prefix varchar,
    geolocation_lat varchar,
    geolocation_lng varchar,
    geolocation_city varchar,
    geolocation_state varchar
);
COPY ECOMMERCE.olist_geolocation(geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state)
FROM '/data/olist_geolocation.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_order_items
(
    order_id varchar,
    order_item_id int,
    product_id varchar,
    seller_id varchar,
    shipping_limit_date timestamp,
    price float,
    freight_value float
);
COPY ECOMMERCE.olist_order_items(order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value)
FROM '/data/olist_order_items.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_order_payments
(
    order_id varchar,
    payment_sequential int,
    payment_type varchar,
    payment_installments int,
    payment_value float
);
COPY ECOMMERCE.olist_order_payments(order_id,payment_sequential,payment_type,payment_installments,payment_value)
FROM '/data/olist_order_payments.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_order_reviews
(
    review_id varchar,
    order_id varchar,
    review_score int,
    review_comment_title varchar,
    review_comment_message varchar,
    review_creation_date timestamp,
    review_answer_timestamp timestamp
);
COPY ECOMMERCE.olist_order_reviews(review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp)
FROM '/data/olist_order_reviews.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_orders
(
    order_id varchar,
    customer_id varchar,
    order_status varchar,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);
COPY ECOMMERCE.olist_orders(order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date)
FROM '/data/olist_orders.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_products
(
    product_id varchar,
    product_category_name varchar,
    product_name_lenght int,
    product_description_lenght int,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int
);
COPY ECOMMERCE.olist_products(product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm)
FROM '/data/olist_products.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.olist_sellers
(
    seller_id varchar,
    seller_zip_code_prefix varchar,
    seller_city varchar,
    seller_state varchar
);
COPY ECOMMERCE.olist_sellers(seller_id,seller_zip_code_prefix,seller_city,seller_state)
FROM '/data/olist_sellers.csv' DELIMITER ',' CSV HEADER;


create table if not exists ECOMMERCE.product_category_name_translation
(
    product_category_name varchar,
    product_category_name_english varchar
);
COPY ECOMMERCE.product_category_name_translation(product_category_name,product_category_name_english)
FROM '/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;