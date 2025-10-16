--!> Create 'CHANNELS' table
Drop Table if EXISTS channels

CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    channel_name VARCHAR NOT NULL,
    channel_type VARCHAR NOT NULL
);

--?> Also create 'CHANNELS_RAW_' table for raw data ingestion
Drop Table if EXISTS channels_raw_

CREATE TABLE IF NOT EXISTS channels_raw_ (
    channel_id text,
    channel_name text,
    channel_type text
);


--!> Create 'DELIVERIES' table
Drop Table if EXISTS deliveries

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id INTEGER PRIMARY KEY,
    delivery_order_id INTEGER NOT NULL,
    driver_id INTEGER NOT NULL,
    delivery_distance_meters INTEGER NOT NULL,
    delivery_status VARCHAR NOT NULL
);

--?> Also create 'DELIVERIES_RAW_' table for raw data ingestion
Drop Table if EXISTS deliveries_raw_

CREATE TABLE IF NOT EXISTS deliveries_raw_ (
    delivery_id text,
    delivery_order_id text,
    driver_id text,
    delivery_distance_meters text,
    delivery_status text
);


--!> Create 'DRIVERS' table
Drop Table if EXISTS drivers

CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY,
    driver_modal VARCHAR NOT NULL,
    driver_type VARCHAR NOT NULL
);

--?> During FK creation, we found that some driver_id in deliveries do not exist in drivers table.
--?> So we will create a dummy drivers with 'Unknown' modal and type for those driver_id.
Insert Into drivers (driver_id, driver_modal, driver_type)
VALUES (-1, 'Unknown', 'Unknown')

--?> Also create 'DRIVERS_RAW_' table for raw data ingestion
Drop Table if EXISTS drivers_raw_

CREATE TABLE IF NOT EXISTS drivers_raw_ (
    driver_id text,
    driver_modal text,
    driver_type text
);



--!> Create 'ORDERS' table
Drop Table if EXISTS orders

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    store_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    payment_order_id INTEGER NOT NULL,
    delivery_order_id INTEGER NOT NULL,
    order_status VARCHAR NOT NULL,
    order_amount DECIMAL NOT NULL,
    order_delivery_fee DECIMAL NOT NULL,
    order_delivery_cost DECIMAL NOT NULL,
    order_created_hour INTEGER NOT NULL,
    order_created_minute INTEGER NOT NULL,
    order_created_day INTEGER NOT NULL,
    order_created_month INTEGER NOT NULL,
    order_created_year INTEGER NOT NULL,
    order_moment_created TIMESTAMP NOT NULL,
    order_moment_accepted TIMESTAMP,
    order_moment_ready TIMESTAMP,
    order_moment_collected TIMESTAMP,
    order_moment_in_expedition TIMESTAMP,
    order_moment_delivering TIMESTAMP,
    order_moment_delivered TIMESTAMP,
    order_moment_finished TIMESTAMP,
    order_metric_collected_time INTEGER,
    order_metric_paused_time INTEGER,
    order_metric_production_time INTEGER,
    order_metric_walking_time INTEGER,
    order_metric_expedition_speed_time INTEGER,
    order_metric_transit_time INTEGER,
    order_metric_cycle_time INTEGER
);

--?> Also create 'ORDERS_RAW_' table for raw data ingestion
Drop Table if EXISTS orders_raw_

CREATE TABLE IF NOT EXISTS orders_raw_ (
    order_id text,
    store_id text,
    channel_id text,
    payment_order_id text,
    delivery_order_id text,
    order_status text,
    order_amount text,
    order_delivery_fee text,
    order_delivery_cost text,
    order_created_hour text,
    order_created_minute text,
    order_created_day text,
    order_created_month text,
    order_created_year text,
    order_moment_created text,
    order_moment_accepted text,
    order_moment_ready text,
    order_moment_collected text,
    order_moment_in_expedition text,
    order_moment_delivering text,
    order_moment_delivered text,
    order_moment_finished text,
    order_metric_collected_time text,
    order_metric_paused_time text,
    order_metric_production_time text,
    order_metric_walking_time text,
    order_metric_expedition_speed_time text,
    order_metric_transit_time text,
    order_metric_cycle_time text
);


--!> Create 'HUBS' table
Drop Table if EXISTS hubs

CREATE TABLE IF NOT EXISTS hubs (
    hub_id INTEGER PRIMARY KEY,
    hub_name VARCHAR NOT NULL,
    hub_city VARCHAR NOT NULL,
    hub_state VARCHAR NOT NULL,
    hub_latitude DECIMAL NOT NULL,
    hub_longitude DECIMAL NOT NULL
);

--?> Also create 'HUBS_RAW_' table for raw data ingestion
Drop Table if EXISTS hubs_raw_

CREATE TABLE IF NOT EXISTS hubs_raw_ (
    hub_id text,
    hub_name text,
    hub_city text,
    hub_state text,
    hub_latitude text,
    hub_longitude text
);


--!> Create 'PAYMENTS' table
Drop Table if EXISTS payments

CREATE TABLE IF NOT EXISTS payments (
    payment_id INTEGER PRIMARY KEY,
    payment_order_id INTEGER NOT NULL,
    payment_amount DECIMAL NOT NULL,
    payment_fee DECIMAL NOT NULL,
    payment_method VARCHAR NOT NULL,
    payment_status VARCHAR NOT NULL
);

--?> Also create 'PAYMENTS_RAW_' table for raw data ingestion
Drop Table if EXISTS payments_raw_

CREATE TABLE IF NOT EXISTS payments_raw_ (
    payment_id text,
    payment_order_id text,
    payment_amount text,
    payment_fee text,
    payment_method text,
    payment_status text
);


--!> Create 'STORES' table
Drop Table if EXISTS stores

CREATE TABLE IF NOT EXISTS stores (
    store_id INTEGER PRIMARY KEY,
    hub_id INTEGER NOT NULL,
    store_name VARCHAR NOT NULL,
    store_segment VARCHAR NOT NULL,
    store_plan_price DECIMAL NOT NULL,
    store_latitude DECIMAL,
    store_longitude DECIMAL
);

--?> Also create 'STORES_RAW_' table for raw data ingestion
Drop Table if EXISTS stores_raw_

CREATE TABLE IF NOT EXISTS stores_raw_ (
    store_id text,
    hub_id text,
    store_name text,
    store_segment text,
    store_plan_price text,
    store_latitude text,
    store_longitude text
);