--!> Preview the data in the raw tables
SELECT * FROM channels_raw_ LIMIT 10;
SELECT * FROM deliveries_raw_ LIMIT 10;
SELECT * FROM drivers_raw_ LIMIT 10;
SELECT * FROM hubs_raw_ LIMIT 10;
SELECT * FROM orders_raw_ LIMIT 10;
SELECT * FROM payments_raw_ LIMIT 10;
SELECT * FROM stores_raw_ LIMIT 10;


--!> Clean 'CHANNELS_RAW_' table and insert cleaned data into 'CHANNELS' table
--?> 1. Identify and count rows with NULL values
Select count(*)
From channels_raw_
WHERE channel_id IS NULL
    OR channel_name IS NULL
    OR channel_type IS NULL

--?> 2. Check for duplicate rows based on 'channel_id'
Select channel_id, count(*)
From channels_raw_
Group by channel_id
Having count(*) > 1

--?> 3. Clean the Data Using with SELECT statement
Select DISTINCT
    channel_id::INTEGER,
    trim(channel_name) as channel_name,
    trim(channel_type) as channel_type
From channels_raw_
Where channel_id IS NOT NULL
    AND channel_name IS NOT NULL
    AND channel_type IS NOT NULL
Order by channel_id ASC


--?> 4. Insert cleaned data into 'CHANNELS' table
Insert into channels (channel_id, channel_name, channel_type)
Select DISTINCT
    channel_id::INTEGER,
    trim(channel_name) as channel_name,
    trim(channel_type) as channel_type
From channels_raw_
Where channel_id is not null
    and channel_name is not null
    and channel_type is not null
Order by channel_id ASC

--?> 5. Verify the insertion
Select * From channels LIMIT 10;


--!> Clean 'DELIVERIES_RAW_' table and insert cleaned data into 'DELIVERIES' table
--?> 1. Clean the Data Using CTEs and Insert into 'DELIVERIES' table
With no_nulls as (
    Select *
    From deliveries_raw_
    Where delivery_id is not NULL
        and delivery_order_id is not NULL
        and driver_id is not NULL
        and delivery_distance_meters is not NULL
        and delivery_status is not NULL
    Order By delivery_id ASC
), no_duplicates as (
    Select DISTINCT *
    From no_nulls
), cleaned_data as (
    Select 
        delivery_id::INTEGER,
        delivery_order_id::INTEGER,
        COALESCE(NULLIF(driver_id, '')::INTEGER, -1) as driver_id,                                  --? Replace empty strings with -1
        COALESCE(NULLIF (delivery_distance_meters, '')::INTEGER, 0) as delivery_distance_meters,    --? Replace empty strings with 0
        trim(delivery_status) as delivery_status
    From no_duplicates
)
Insert into deliveries(delivery_id, delivery_order_id, driver_id, delivery_distance_meters, delivery_status)       
Select *
From cleaned_data

--?> 2. Verify the insertion
Select * From deliveries LIMIT 10;


--!> Clean 'DRIVERS_RAW_' table and insert cleaned data into 'DRIVERS' table
--?> 1. Clean the Data Using CTEs and Insert into 'DRIVERS' table
With no_nulls as (
    Select *
    From drivers_raw_
    Where driver_id is not NULL
        and driver_modal is not NULL
        and driver_type is not NULL
    Order By driver_id ASC
), no_duplicates as (
    Select DISTINCT *
    From no_nulls
), cleaned_data as (
    Select 
        driver_id::INTEGER,
        trim(driver_modal) as driver_modal,
        trim(driver_type) as driver_type
    From no_duplicates
)
Insert into drivers(driver_id, driver_modal, driver_type)
Select *
From cleaned_data 

--?> 2. Verify the insertion
Select * From drivers LIMIT 10;


--!> Clean 'HUBS_RAW_' table and insert cleaned data into 'HUBS' table
--?> 1. Clean the Data Using CTEs and Insert into 'HUBS table
With no_nulls as (
    Select *
    From hubs_raw_
    Where hub_id is not NULL
        and hub_name is not NULL
        and hub_city is not NULL
        and hub_state is not NULL
        and hub_latitude is not NULL
        and hub_longitude is not NULL
    Order By hub_id ASC
), no_duplicates as (
    Select DISTINCT *
    From no_nulls
), cleaned_data as (
    Select 
        hub_id::INTEGER,
        trim(hub_name) as hub_name,
        trim(replace(hub_city, '�', 'A')) as hub_city,
        trim(hub_state) as hub_state,
        hub_latitude::FLOAT,
        hub_longitude::FLOAT
    From no_duplicates
)
Insert into hubs(hub_id, hub_name, hub_city, hub_state, hub_latitude, hub_longitude)
Select *
From cleaned_data

--?> 2. Verify the insertion
Select * From hubs LIMIT 10;


--!> Clean 'ORDERS_RAW_' table and insert cleaned data into 'ORDERS' table
--?> 1. Clean the Data Using CTEs and Insert into 'ORDERS'
With no_nulls as (
    Select *
    From orders_raw_
    Where order_id is not NULL
        and store_id is not NULL
        and channel_id is not NULL
        and payment_order_id is not NULL
        and delivery_order_id is not NULL
        and order_status is not NULL
        and order_amount is not NULL
        and order_delivery_fee is not NULL
        and order_delivery_cost is not NULL
        and order_created_hour is not NULL
        and order_created_minute is not NULL
        and order_created_day is not NULL
        and order_created_month is not NULL
        and order_created_year is not NULL
        and order_moment_created is not NULL
        and order_moment_accepted is not NULL
        and order_moment_ready is not NULL
        and order_moment_collected is not NULL
        and order_moment_in_expedition is not NULL
        and order_moment_delivering is not NULL
        and order_moment_delivered is not NULL
        and order_moment_finished is not NULL
        and order_metric_collected_time is not NULL
        and order_metric_paused_time is not NULL
        and order_metric_production_time is not NULL
        and order_metric_walking_time is not NULL
        and order_metric_transit_time is not NULL
        and order_metric_cycle_time is not NULL
    Order By order_id ASC
), no_duplicates as (
    Select DISTINCT *
    From no_nulls
), cleaned_data as (
    SELECT 
        order_id::INTEGER,
        store_id::INTEGER,
        channel_id::INTEGER,
        payment_order_id::INTEGER,
        delivery_order_id::INTEGER,
        TRIM(order_status) AS order_status,

        COALESCE(NULLIF(order_amount, '')::FLOAT, 0.0) AS order_amount,
        COALESCE(NULLIF(order_delivery_fee, '')::FLOAT, 0.0) AS order_delivery_fee,
        COALESCE(NULLIF(order_delivery_cost, '')::FLOAT, 0.0) AS order_delivery_cost,

        order_created_hour::INTEGER,
        order_created_minute::INTEGER,
        order_created_day::INTEGER,
        order_created_month::INTEGER,
        order_created_year::INTEGER,

        CASE 
            WHEN order_moment_created = '' THEN NULL
            ELSE to_timestamp(order_moment_created, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_created,

        CASE 
            WHEN order_moment_accepted = '' THEN NULL
            ELSE to_timestamp(order_moment_accepted, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_accepted,

        CASE 
            WHEN order_moment_ready = '' THEN NULL
            ELSE to_timestamp(order_moment_ready, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_ready,

        CASE 
            WHEN order_moment_collected = '' THEN NULL
            ELSE to_timestamp(order_moment_collected, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_collected,

        CASE 
            WHEN order_moment_in_expedition = '' THEN NULL
            ELSE to_timestamp(order_moment_in_expedition, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_in_expedition,

        CASE 
            WHEN order_moment_delivering = '' THEN NULL
            ELSE to_timestamp(order_moment_delivering, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_delivering,

        CASE 
            WHEN order_moment_delivered = '' THEN NULL
            ELSE to_timestamp(order_moment_delivered, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_delivered,

        CASE 
            WHEN order_moment_finished = '' THEN NULL
            ELSE to_timestamp(order_moment_finished, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_finished,
        COALESCE(NULLIF(order_metric_collected_time, '')::FLOAT, 0.0) AS order_metric_collected_time,
        COALESCE(NULLIF(order_metric_paused_time, '')::FLOAT, 0.0) AS order_metric_paused_time,
        COALESCE(NULLIF(order_metric_production_time, '')::FLOAT, 0.0) AS order_metric_production_time,
        COALESCE(NULLIF(order_metric_walking_time, '')::FLOAT, 0.0) AS order_metric_walking_time,
        COALESCE(NULLIF(order_metric_expedition_speed_time, '')::FLOAT, 0.0) AS order_metric_expedition_speed_time,
        COALESCE(NULLIF(order_metric_transit_time, '')::FLOAT, 0.0) AS order_metric_transit_time,
        COALESCE(NULLIF(order_metric_cycle_time, '')::FLOAT, 0.0) AS order_metric_cycle_time
    FROM no_duplicates
)
Insert into orders(
    order_id, store_id, channel_id, payment_order_id, delivery_order_id, order_status,
    order_amount, order_delivery_fee, order_delivery_cost,
    order_created_hour, order_created_minute, order_created_day, order_created_month, order_created_year,
    order_moment_created, order_moment_accepted, order_moment_ready, order_moment_collected,
    order_moment_in_expedition, order_moment_delivering, order_moment_delivered, order_moment_finished,
    order_metric_collected_time, order_metric_paused_time, order_metric_production_time,
    order_metric_walking_time, order_metric_expedition_speed_time, order_metric_transit_time,
    order_metric_cycle_time)
Select *
From cleaned_data

--?> 2. Verify the insertion
Select * From orders LIMIT 10;


--!> Clean 'PAYMENTS_RAW_' table and insert cleaned data into 'PAYMENTS' table
--?> 1. Clean the Data Using CTEs and Insert into 'PAYMENTS'
With no_nulls as (
    Select *
    From payments_raw_
    Where payment_id is not NULL
        and payment_order_id is not NULL
        and payment_amount is not NULL
        and payment_fee is not NULL
        and payment_method is not NULL
        and payment_status is not NULL
    Order By payment_id ASC
), no_duplicates as (
    Select DISTINCT *
    From no_nulls
), cleaned_data as (
    Select 
        payment_id::INTEGER,
        payment_order_id::INTEGER,
        COALESCE(NULLIF(payment_amount, '')::FLOAT, 0.0) as payment_amount,
        COALESCE(NULLIF(payment_fee, '')::FLOAT, 0.0) as payment_fee,
        upper(trim(payment_method)) as payment_method,
        upper(trim(payment_status)) as payment_status
    From no_duplicates
)
Insert into payments(payment_id, payment_order_id, payment_amount, payment_fee, payment_method, payment_status)
Select *
From cleaned_data

--?> 2. Verify the insertion
Select * From payments LIMIT 10;


--!> Clean 'STORES_RAW_' table and insert cleaned data into 'STORES' table
--?> 1. Clean the Data Using CTEs and Insert into 'STORES'
With no_nulls as (
    Select *
    From stores_raw_
    Where store_id is not NULL
        and hub_id is not NULL
        and store_name is not NULL
        and store_segment is not NULL
        and store_plan_price is not NULL
        and store_latitude is not NULL
        and store_longitude is not NULL
    Order By store_id ASC
), no_duplicates as (
    Select DISTINCT *
    From no_nulls
), cleaned_data as (
    Select
        store_id::INTEGER,
        hub_id::INTEGER,
        upper(trim(store_name)) as store_name,
        upper(trim(store_segment)) as store_segment,
        COALESCE(NULLIF(store_plan_price, '')::FLOAT, -1.0) as store_plan_price,
        NULLIF(TRIM(store_latitude), '')::FLOAT as store_latitude,
        NULLIF(TRIM(store_latitude), '')::FLOAT as store_longitude
    From no_duplicates
)
Insert into stores(store_id, hub_id, store_name, store_segment, store_plan_price, store_latitude, store_longitude)
Select *
From cleaned_data

--?> 2. Verify the insertion
Select * From stores LIMIT 10;


--!> During the FK creation, I encounter an error related to existing data violating the FK constraint.
--?> To resolve this, I will identify and remove the problematic records from the 'ORDER' table.
--?> 1. Identify problematic records in 'ORDERS' table where payment_order_id or delivery_order_id do not match order_id
SELECT *
FROM orders
WHERE delivery_order_id IS DISTINCT FROM order_id
    OR payment_order_id IS DISTINCT FROM order_id;

--?> 2. Delete problematic records from 'ORDERS' table
Alter Table orders
Drop Column delivery_order_id,
Drop Column payment_order_id;