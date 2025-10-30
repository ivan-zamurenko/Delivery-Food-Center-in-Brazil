-- Task B: Delivery Time Optimization & Driver Analysis
-- SQL queries for operational efficiency and driver performance analysis
-- Author: Ivan Zamurenko
-- Date: October 30, 2025

-- ==========================================
-- Query 1: Average Delivery Time by Driver Modal
-- ==========================================
-- Calculate average delivery time for each driver modal type
-- (BIKER, MOTOBOY, WALKER, etc.)

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id,
        d.delivery_status
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
)
SELECT
    dr.driver_modal,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS avg_delivery_time_minutes,
    ROUND(MIN(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS min_delivery_time_minutes,
    ROUND(MAX(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS max_delivery_time_minutes,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS median_delivery_time_minutes
FROM delivered_orders del
JOIN drivers dr ON dr.driver_id = del.driver_id
GROUP BY dr.driver_modal
ORDER BY AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60) ASC;


-- ==========================================
-- Query 2: Average Delivery Time by Driver Type
-- ==========================================
-- Calculate average delivery time for each driver type

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id,
        d.delivery_status
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
)
SELECT
    dr.driver_type,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS avg_delivery_time_minutes,
    ROUND(STDDEV(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS stddev_delivery_time
FROM delivered_orders del
JOIN drivers dr ON dr.driver_id = del.driver_id
GROUP BY dr.driver_type
ORDER BY AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60) ASC;


-- ==========================================
-- Query 3: Individual Driver Performance
-- ==========================================
-- Detailed performance metrics for each driver

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
)
SELECT 
    dr.driver_id,
    dr.driver_modal,
    dr.driver_type,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS avg_delivery_time_minutes,
    ROUND(MIN(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS min_delivery_time,
    ROUND(MAX(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS max_delivery_time,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS q1_delivery_time,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS median_delivery_time,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS q3_delivery_time
FROM delivered_orders del
JOIN drivers dr ON dr.driver_id = del.driver_id
GROUP BY dr.driver_id, dr.driver_modal, dr.driver_type
ORDER BY AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60) ASC;


-- ==========================================
-- Query 4: Top 10 Fastest Drivers
-- ==========================================
-- Identify the fastest drivers (minimum 50 deliveries for statistical significance)

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
),
driver_performance AS (
    SELECT 
        dr.driver_id,
        dr.driver_modal,
        dr.driver_type,
        COUNT(*) AS total_deliveries,
        ROUND(AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS avg_delivery_time_minutes
    FROM delivered_orders del
    JOIN drivers dr ON dr.driver_id = del.driver_id
    GROUP BY dr.driver_id, dr.driver_modal, dr.driver_type
    HAVING COUNT(*) >= 50  -- Minimum 50 deliveries
)
SELECT 
    driver_id,
    driver_modal,
    driver_type,
    total_deliveries,
    avg_delivery_time_minutes,
    RANK() OVER (ORDER BY avg_delivery_time_minutes ASC) AS speed_rank
FROM driver_performance
ORDER BY avg_delivery_time_minutes ASC
LIMIT 10;


-- ==========================================
-- Query 5: Delivery Status Distribution
-- ==========================================
-- Analyze delivery success rate by driver modal

SELECT 
    dr.driver_modal,
    d.delivery_status,
    COUNT(*) AS status_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY dr.driver_modal) * 100, 2) AS percentage_of_modal
FROM deliveries d
JOIN drivers dr ON dr.driver_id = d.driver_id
GROUP BY dr.driver_modal, d.delivery_status
ORDER BY dr.driver_modal, COUNT(*) DESC;


-- ==========================================
-- Query 6: Delivery Time Distribution by Time of Day
-- ==========================================
-- Analyze how delivery times vary throughout the day

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id,
        EXTRACT(HOUR FROM o.order_moment_delivering) AS delivery_hour
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
),
hourly_performance AS (
    SELECT 
        del.delivery_hour,
        dr.driver_modal,
        COUNT(*) AS delivery_count,
        ROUND(AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS avg_delivery_time_minutes
    FROM delivered_orders del
    JOIN drivers dr ON dr.driver_id = del.driver_id
    GROUP BY del.delivery_hour, dr.driver_modal
)
SELECT 
    delivery_hour,
    driver_modal,
    delivery_count,
    avg_delivery_time_minutes
FROM hourly_performance
ORDER BY delivery_hour, driver_modal;


-- ==========================================
-- Query 7: Outlier Detection - Unusually Long Deliveries
-- ==========================================
-- Identify deliveries that took significantly longer than average

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.store_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id,
        EXTRACT(EPOCH FROM (o.order_moment_delivered - o.order_moment_delivering)) / 60 AS delivery_time_minutes
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
),
driver_stats AS (
    SELECT 
        driver_id,
        AVG(delivery_time_minutes) AS avg_time,
        STDDEV(delivery_time_minutes) AS stddev_time
    FROM delivered_orders
    GROUP BY driver_id
)
SELECT 
    del.order_id,
    del.store_id,
    del.driver_id,
    dr.driver_modal,
    dr.driver_type,
    ROUND(del.delivery_time_minutes::NUMERIC, 2) AS delivery_time_minutes,
    ROUND(ds.avg_time::NUMERIC, 2) AS driver_avg_time,
    ROUND(((del.delivery_time_minutes - ds.avg_time) / ds.stddev_time)::NUMERIC, 2) AS z_score
FROM delivered_orders del
JOIN drivers dr ON dr.driver_id = del.driver_id
JOIN driver_stats ds ON ds.driver_id = del.driver_id
WHERE (del.delivery_time_minutes - ds.avg_time) / ds.stddev_time > 3  -- More than 3 standard deviations
    AND ds.stddev_time > 0
ORDER BY (del.delivery_time_minutes - ds.avg_time) / ds.stddev_time DESC;


-- ==========================================
-- Query 8: Driver Modal Comparison Matrix
-- ==========================================
-- Compare all driver modals side-by-side with key metrics

WITH delivered_orders AS (
    SELECT 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
)
SELECT 
    dr.driver_modal,
    COUNT(DISTINCT dr.driver_id) AS unique_drivers,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS avg_delivery_time,
    ROUND(STDDEV(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS stddev_time,
    ROUND(MIN(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS min_time,
    ROUND(MAX(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS max_time,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS q1_time,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS median_time,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60)::NUMERIC, 2) AS q3_time
FROM delivered_orders del
JOIN drivers dr ON dr.driver_id = del.driver_id
GROUP BY dr.driver_modal
ORDER BY AVG(EXTRACT(EPOCH FROM (del.order_moment_delivered - del.order_moment_delivering)) / 60) ASC;


-- ==========================================
-- Query 9: Failed Delivery Analysis
-- ==========================================
-- Analyze failed deliveries by driver modal and type

SELECT 
    dr.driver_modal,
    dr.driver_type,
    COUNT(*) AS failed_deliveries,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) AS percentage_of_total_failures
FROM deliveries d
JOIN drivers dr ON dr.driver_id = d.driver_id
WHERE UPPER(d.delivery_status) != 'DELIVERED'
GROUP BY dr.driver_modal, dr.driver_type
ORDER BY COUNT(*) DESC;


-- ==========================================
-- Query 10: Driver Efficiency Score
-- ==========================================
-- Calculate a composite efficiency score based on speed and reliability

WITH delivered_orders AS (
    SELECT 
        d.driver_id,
        EXTRACT(EPOCH FROM (o.order_moment_delivered - o.order_moment_delivering)) / 60 AS delivery_time_minutes
    FROM orders o
    JOIN deliveries d ON d.delivery_order_id = o.order_id
    WHERE 
        UPPER(d.delivery_status) = 'DELIVERED'
        AND o.order_moment_delivering IS NOT NULL
        AND o.order_moment_delivered IS NOT NULL
),
driver_performance AS (
    SELECT 
        driver_id,
        COUNT(*) AS successful_deliveries,
        AVG(delivery_time_minutes) AS avg_delivery_time
    FROM delivered_orders
    GROUP BY driver_id
),
all_deliveries AS (
    SELECT 
        driver_id,
        COUNT(*) AS total_deliveries
    FROM deliveries
    GROUP BY driver_id
)
SELECT 
    dr.driver_id,
    dr.driver_modal,
    dr.driver_type,
    dp.successful_deliveries,
    ad.total_deliveries,
    ROUND((dp.successful_deliveries::NUMERIC / ad.total_deliveries) * 100, 2) AS success_rate,
    ROUND(dp.avg_delivery_time::NUMERIC, 2) AS avg_delivery_time_minutes,
    -- Efficiency score: (success_rate / avg_time) * 100
    ROUND((((dp.successful_deliveries::NUMERIC / ad.total_deliveries) / dp.avg_delivery_time) * 100)::NUMERIC, 2) AS efficiency_score
FROM driver_performance dp
JOIN all_deliveries ad ON ad.driver_id = dp.driver_id
JOIN drivers dr ON dr.driver_id = dp.driver_id
WHERE ad.total_deliveries >= 20  -- Minimum 20 deliveries
ORDER BY ((dp.successful_deliveries::NUMERIC / ad.total_deliveries) / dp.avg_delivery_time) DESC;
