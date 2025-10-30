-- Task D: Data Quality & Pipeline Engineering
-- SQL queries for data cleaning, validation, and quality assurance
-- Author: Ivan Zamurenko
-- Date: October 30, 2025

-- ==========================================
-- SECTION 1: DATA QUALITY CHECKS
-- ==========================================

-- Query 1.1: Preview Raw Tables
-- Quick inspection of raw data before cleaning
SELECT * FROM channels_raw_ LIMIT 10;
SELECT * FROM deliveries_raw_ LIMIT 10;
SELECT * FROM drivers_raw_ LIMIT 10;
SELECT * FROM hubs_raw_ LIMIT 10;
SELECT * FROM orders_raw_ LIMIT 10;
SELECT * FROM payments_raw_ LIMIT 10;
SELECT * FROM stores_raw_ LIMIT 10;


-- Query 1.2: NULL Value Analysis
-- Count NULL values in each critical column for all tables

-- Channels NULL check
SELECT 
    'channels_raw_' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN channel_id IS NULL THEN 1 ELSE 0 END) AS channel_id_nulls,
    SUM(CASE WHEN channel_name IS NULL THEN 1 ELSE 0 END) AS channel_name_nulls,
    SUM(CASE WHEN channel_type IS NULL THEN 1 ELSE 0 END) AS channel_type_nulls
FROM channels_raw_;

-- Payments NULL check
SELECT 
    'payments_raw_' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN payment_id IS NULL THEN 1 ELSE 0 END) AS payment_id_nulls,
    SUM(CASE WHEN payment_order_id IS NULL THEN 1 ELSE 0 END) AS payment_order_id_nulls,
    SUM(CASE WHEN payment_amount IS NULL THEN 1 ELSE 0 END) AS payment_amount_nulls,
    SUM(CASE WHEN payment_fee IS NULL THEN 1 ELSE 0 END) AS payment_fee_nulls,
    SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS payment_method_nulls
FROM payments_raw_;


-- Query 1.3: Duplicate Detection
-- Identify duplicate records based on primary keys

-- Channels duplicates
SELECT 
    channel_id, 
    COUNT(*) AS duplicate_count
FROM channels_raw_
GROUP BY channel_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- Payments duplicates
SELECT 
    payment_id, 
    COUNT(*) AS duplicate_count
FROM payments_raw_
GROUP BY payment_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- Orders duplicates
SELECT 
    order_id, 
    COUNT(*) AS duplicate_count
FROM orders_raw_
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;


-- ==========================================
-- SECTION 2: DATA CLEANING PROCEDURES
-- ==========================================

-- Query 2.1: Clean CHANNELS table
-- Remove NULLs, trim whitespace, cast types, deduplicate
WITH no_nulls AS (
    SELECT *
    FROM channels_raw_
    WHERE channel_id IS NOT NULL
        AND channel_name IS NOT NULL
        AND channel_type IS NOT NULL
),
cleaned_data AS (
    SELECT DISTINCT
        channel_id::INTEGER,
        TRIM(channel_name) AS channel_name,
        TRIM(channel_type) AS channel_type
    FROM no_nulls
)
INSERT INTO channels (channel_id, channel_name, channel_type)
SELECT *
FROM cleaned_data
ORDER BY channel_id ASC
ON CONFLICT (channel_id) DO NOTHING;

-- Verify
SELECT * FROM channels LIMIT 10;


-- Query 2.2: Clean DELIVERIES table
-- Handle NULLs, empty strings, type casting
WITH no_nulls AS (
    SELECT *
    FROM deliveries_raw_
    WHERE delivery_id IS NOT NULL
        AND delivery_order_id IS NOT NULL
        AND driver_id IS NOT NULL
        AND delivery_distance_meters IS NOT NULL
        AND delivery_status IS NOT NULL
), 
no_duplicates AS (
    SELECT DISTINCT *
    FROM no_nulls
),
cleaned_data AS (
    SELECT 
        delivery_id::INTEGER,
        delivery_order_id::INTEGER,
        COALESCE(NULLIF(driver_id, '')::INTEGER, -1) AS driver_id,  -- Replace empty with -1
        COALESCE(NULLIF(delivery_distance_meters, '')::INTEGER, 0) AS delivery_distance_meters,
        TRIM(delivery_status) AS delivery_status
    FROM no_duplicates
)
INSERT INTO deliveries (delivery_id, delivery_order_id, driver_id, delivery_distance_meters, delivery_status)
SELECT *
FROM cleaned_data
ON CONFLICT (delivery_id) DO NOTHING;

-- Verify
SELECT * FROM deliveries LIMIT 10;


-- Query 2.3: Clean DRIVERS table
-- Standard cleaning: NULL removal, deduplication, trimming
WITH no_nulls AS (
    SELECT *
    FROM drivers_raw_
    WHERE driver_id IS NOT NULL
        AND driver_modal IS NOT NULL
        AND driver_type IS NOT NULL
),
no_duplicates AS (
    SELECT DISTINCT *
    FROM no_nulls
),
cleaned_data AS (
    SELECT 
        driver_id::INTEGER,
        TRIM(driver_modal) AS driver_modal,
        TRIM(driver_type) AS driver_type
    FROM no_duplicates
)
INSERT INTO drivers (driver_id, driver_modal, driver_type)
SELECT *
FROM cleaned_data
ON CONFLICT (driver_id) DO NOTHING;

-- Verify
SELECT * FROM drivers LIMIT 10;


-- Query 2.4: Clean HUBS table
-- Handle encoding issues (Brazilian Portuguese characters)
WITH no_nulls AS (
    SELECT *
    FROM hubs_raw_
    WHERE hub_id IS NOT NULL
        AND hub_name IS NOT NULL
        AND hub_city IS NOT NULL
        AND hub_state IS NOT NULL
        AND hub_latitude IS NOT NULL
        AND hub_longitude IS NOT NULL
),
no_duplicates AS (
    SELECT DISTINCT *
    FROM no_nulls
),
cleaned_data AS (
    SELECT 
        hub_id::INTEGER,
        TRIM(hub_name) AS hub_name,
        TRIM(REPLACE(hub_city, '�', 'A')) AS hub_city,  -- Fix encoding issues
        TRIM(hub_state) AS hub_state,
        hub_latitude::FLOAT,
        hub_longitude::FLOAT
    FROM no_duplicates
)
INSERT INTO hubs (hub_id, hub_name, hub_city, hub_state, hub_latitude, hub_longitude)
SELECT *
FROM cleaned_data
ON CONFLICT (hub_id) DO NOTHING;

-- Verify
SELECT * FROM hubs LIMIT 10;


-- Query 2.5: Clean PAYMENTS table
-- Standardize payment methods (uppercase), handle decimals
WITH no_nulls AS (
    SELECT *
    FROM payments_raw_
    WHERE payment_id IS NOT NULL
        AND payment_order_id IS NOT NULL
        AND payment_amount IS NOT NULL
        AND payment_fee IS NOT NULL
        AND payment_method IS NOT NULL
        AND payment_status IS NOT NULL
),
no_duplicates AS (
    SELECT DISTINCT *
    FROM no_nulls
),
cleaned_data AS (
    SELECT 
        payment_id::INTEGER,
        payment_order_id::INTEGER,
        COALESCE(NULLIF(payment_amount, '')::FLOAT, 0.0) AS payment_amount,
        COALESCE(NULLIF(payment_fee, '')::FLOAT, 0.0) AS payment_fee,
        UPPER(TRIM(payment_method)) AS payment_method,
        UPPER(TRIM(payment_status)) AS payment_status
    FROM no_duplicates
)
INSERT INTO payments (payment_id, payment_order_id, payment_amount, payment_fee, payment_method, payment_status)
SELECT *
FROM cleaned_data
ON CONFLICT (payment_id) DO NOTHING;

-- Verify
SELECT * FROM payments LIMIT 10;


-- Query 2.6: Clean STORES table
-- Handle geographic coordinates, standardize text
WITH no_nulls AS (
    SELECT *
    FROM stores_raw_
    WHERE store_id IS NOT NULL
        AND hub_id IS NOT NULL
        AND store_name IS NOT NULL
        AND store_segment IS NOT NULL
        AND store_plan_price IS NOT NULL
        AND store_latitude IS NOT NULL
        AND store_longitude IS NOT NULL
),
no_duplicates AS (
    SELECT DISTINCT *
    FROM no_nulls
),
cleaned_data AS (
    SELECT
        store_id::INTEGER,
        hub_id::INTEGER,
        UPPER(TRIM(store_name)) AS store_name,
        UPPER(TRIM(store_segment)) AS store_segment,
        COALESCE(NULLIF(store_plan_price, '')::FLOAT, -1.0) AS store_plan_price,
        NULLIF(TRIM(store_latitude), '')::FLOAT AS store_latitude,
        NULLIF(TRIM(store_longitude), '')::FLOAT AS store_longitude
    FROM no_duplicates
)
INSERT INTO stores (store_id, hub_id, store_name, store_segment, store_plan_price, store_latitude, store_longitude)
SELECT *
FROM cleaned_data
ON CONFLICT (store_id) DO NOTHING;

-- Verify
SELECT * FROM stores LIMIT 10;


-- Query 2.7: Clean ORDERS table (Complex - Multiple Timestamps)
-- Handle timestamp parsing, NULL coalescing, empty string handling
WITH no_nulls AS (
    SELECT *
    FROM orders_raw_
    WHERE order_id IS NOT NULL
        AND store_id IS NOT NULL
        AND channel_id IS NOT NULL
        AND order_status IS NOT NULL
        AND order_created_year IS NOT NULL
),
no_duplicates AS (
    SELECT DISTINCT *
    FROM no_nulls
),
cleaned_data AS (
    SELECT 
        order_id::INTEGER,
        store_id::INTEGER,
        channel_id::INTEGER,
        TRIM(order_status) AS order_status,
        COALESCE(NULLIF(order_amount, '')::FLOAT, 0.0) AS order_amount,
        COALESCE(NULLIF(order_delivery_fee, '')::FLOAT, 0.0) AS order_delivery_fee,
        COALESCE(NULLIF(order_delivery_cost, '')::FLOAT, 0.0) AS order_delivery_cost,
        order_created_hour::INTEGER,
        order_created_minute::INTEGER,
        order_created_day::INTEGER,
        order_created_month::INTEGER,
        order_created_year::INTEGER,
        
        -- Parse timestamps with error handling
        CASE 
            WHEN order_moment_created = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_created, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_created,
        
        CASE 
            WHEN order_moment_accepted = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_accepted, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_accepted,
        
        CASE 
            WHEN order_moment_ready = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_ready, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_ready,
        
        CASE 
            WHEN order_moment_collected = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_collected, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_collected,
        
        CASE 
            WHEN order_moment_in_expedition = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_in_expedition, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_in_expedition,
        
        CASE 
            WHEN order_moment_delivering = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_delivering, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_delivering,
        
        CASE 
            WHEN order_moment_delivered = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_delivered, 'MM/DD/YYYY HH12:MI:SS AM')
        END AS order_moment_delivered,
        
        CASE 
            WHEN order_moment_finished = '' THEN NULL
            ELSE TO_TIMESTAMP(order_moment_finished, 'MM/DD/YYYY HH12:MI:SS AM')
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
INSERT INTO orders (
    order_id, store_id, channel_id, order_status,
    order_amount, order_delivery_fee, order_delivery_cost,
    order_created_hour, order_created_minute, order_created_day, order_created_month, order_created_year,
    order_moment_created, order_moment_accepted, order_moment_ready, order_moment_collected,
    order_moment_in_expedition, order_moment_delivering, order_moment_delivered, order_moment_finished,
    order_metric_collected_time, order_metric_paused_time, order_metric_production_time,
    order_metric_walking_time, order_metric_expedition_speed_time, order_metric_transit_time,
    order_metric_cycle_time
)
SELECT *
FROM cleaned_data
ON CONFLICT (order_id) DO NOTHING;

-- Verify
SELECT * FROM orders LIMIT 10;


-- ==========================================
-- SECTION 3: REFERENTIAL INTEGRITY VALIDATION
-- ==========================================

-- Query 3.1: Orphaned Records Detection
-- Find records that violate foreign key constraints

-- Orders with non-existent stores
SELECT 
    'Orders → Stores' AS relationship,
    COUNT(*) AS orphaned_count
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM stores s WHERE s.store_id = o.store_id
);

-- Orders with non-existent channels
SELECT 
    'Orders → Channels' AS relationship,
    COUNT(*) AS orphaned_count
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM channels c WHERE c.channel_id = o.channel_id
);

-- Deliveries with non-existent orders
SELECT 
    'Deliveries → Orders' AS relationship,
    COUNT(*) AS orphaned_count
FROM deliveries d
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.order_id = d.delivery_order_id
);

-- Deliveries with non-existent drivers
SELECT 
    'Deliveries → Drivers' AS relationship,
    COUNT(*) AS orphaned_count
FROM deliveries d
WHERE NOT EXISTS (
    SELECT 1 FROM drivers dr WHERE dr.driver_id = d.driver_id
);

-- Payments with non-existent orders
SELECT 
    'Payments → Orders' AS relationship,
    COUNT(*) AS orphaned_count
FROM payments p
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.order_id = p.payment_order_id
);

-- Stores with non-existent hubs
SELECT 
    'Stores → Hubs' AS relationship,
    COUNT(*) AS orphaned_count
FROM stores s
WHERE NOT EXISTS (
    SELECT 1 FROM hubs h WHERE h.hub_id = s.hub_id
);


-- Query 3.2: Delete Orphaned Records
-- Remove records that prevent foreign key creation

-- Delete orders with invalid store_id
DELETE FROM orders
WHERE store_id NOT IN (SELECT store_id FROM stores);

-- Delete orders with invalid channel_id
DELETE FROM orders
WHERE channel_id NOT IN (SELECT channel_id FROM channels);

-- Delete deliveries with invalid order_id
DELETE FROM deliveries
WHERE delivery_order_id NOT IN (SELECT order_id FROM orders);

-- Delete payments with invalid order_id
DELETE FROM payments
WHERE payment_order_id NOT IN (SELECT order_id FROM orders);


-- ==========================================
-- SECTION 4: DATA QUALITY METRICS
-- ==========================================

-- Query 4.1: Data Completeness Report
-- Calculate percentage of NULL values in each table

SELECT 
    'channels' AS table_name,
    COUNT(*) AS total_rows,
    ROUND((COUNT(*) - COUNT(channel_id))::NUMERIC / COUNT(*) * 100, 2) AS channel_id_null_pct,
    ROUND((COUNT(*) - COUNT(channel_name))::NUMERIC / COUNT(*) * 100, 2) AS channel_name_null_pct
FROM channels;

SELECT 
    'payments' AS table_name,
    COUNT(*) AS total_rows,
    ROUND((COUNT(*) - COUNT(payment_amount))::NUMERIC / COUNT(*) * 100, 2) AS payment_amount_null_pct,
    ROUND((COUNT(*) - COUNT(payment_method))::NUMERIC / COUNT(*) * 100, 2) AS payment_method_null_pct
FROM payments;


-- Query 4.2: Data Retention Rate
-- Compare raw vs cleaned record counts

SELECT 
    'channels' AS table_name,
    (SELECT COUNT(*) FROM channels_raw_) AS raw_count,
    (SELECT COUNT(*) FROM channels) AS cleaned_count,
    ROUND((SELECT COUNT(*)::NUMERIC FROM channels) / (SELECT COUNT(*) FROM channels_raw_) * 100, 2) AS retention_rate
UNION ALL
SELECT 
    'payments',
    (SELECT COUNT(*) FROM payments_raw_),
    (SELECT COUNT(*) FROM payments),
    ROUND((SELECT COUNT(*)::NUMERIC FROM payments) / (SELECT COUNT(*) FROM payments_raw_) * 100, 2)
UNION ALL
SELECT 
    'orders',
    (SELECT COUNT(*) FROM orders_raw_),
    (SELECT COUNT(*) FROM orders),
    ROUND((SELECT COUNT(*)::NUMERIC FROM orders) / (SELECT COUNT(*) FROM orders_raw_) * 100, 2);


-- Query 4.3: Data Type Validation
-- Verify numeric fields contain valid values

SELECT 
    'payment_amount' AS field,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN payment_amount < 0 THEN 1 END) AS negative_values,
    COUNT(CASE WHEN payment_amount = 0 THEN 1 END) AS zero_values,
    ROUND(AVG(payment_amount), 2) AS avg_value,
    ROUND(MIN(payment_amount), 2) AS min_value,
    ROUND(MAX(payment_amount), 2) AS max_value
FROM payments;


-- Query 4.4: Consistency Check
-- Verify business logic rules

-- Check: payment_amount should be >= payment_fee
SELECT 
    COUNT(*) AS invalid_payment_records
FROM payments
WHERE payment_amount < payment_fee;

-- Check: order_amount should be positive
SELECT 
    COUNT(*) AS negative_order_amounts
FROM orders
WHERE order_amount < 0;

-- Check: delivery timestamps should be in logical order
SELECT 
    COUNT(*) AS invalid_timestamp_sequences
FROM orders
WHERE order_moment_delivered IS NOT NULL 
    AND order_moment_delivering IS NOT NULL
    AND order_moment_delivered < order_moment_delivering;
