-- Task A: Channel & Payment Mix Profitability Analysis
-- SQL queries for revenue optimization and profitability analysis
-- Author: Ivan Zamurenko
-- Date: October 30, 2025

-- ==========================================
-- Query 1: Revenue by Channel & Payment Method
-- ==========================================
-- Calculate total revenue (payment_amount - payment_fee) for each
-- channel + payment method combination to identify most profitable segments

SELECT 
    o.channel_id,
    c.channel_name,
    p.payment_method,
    COUNT(*) AS payments_count,
    SUM(p.payment_amount) AS total_payment_amount,
    SUM(p.payment_fee) AS total_payment_fees,
    SUM(p.payment_amount - p.payment_fee) AS total_revenue
FROM orders o
JOIN payments p ON p.payment_order_id = o.order_id
JOIN channels c ON c.channel_id = o.channel_id
GROUP BY o.channel_id, c.channel_name, p.payment_method
ORDER BY SUM(p.payment_amount - p.payment_fee) DESC;


-- ==========================================
-- Query 2: Top 10 Most Profitable Combinations
-- ==========================================
-- Identify the top 10 channel + payment method combinations
-- that generate the highest revenue

SELECT 
    o.channel_id,
    c.channel_name,
    p.payment_method,
    COUNT(*) AS payments_count,
    SUM(p.payment_amount - p.payment_fee) AS total_revenue,
    ROUND(AVG(p.payment_amount - p.payment_fee), 2) AS avg_revenue_per_transaction
FROM orders o
JOIN payments p ON p.payment_order_id = o.order_id
JOIN channels c ON c.channel_id = o.channel_id
GROUP BY o.channel_id, c.channel_name, p.payment_method
ORDER BY SUM(p.payment_amount - p.payment_fee) DESC
LIMIT 10;


-- ==========================================
-- Query 3: Revenue Share by Channel
-- ==========================================
-- Calculate what percentage of total revenue each channel generates

WITH channel_revenue AS (
    SELECT 
        o.channel_id,
        c.channel_name,
        SUM(p.payment_amount - p.payment_fee) AS channel_revenue
    FROM orders o
    JOIN payments p ON p.payment_order_id = o.order_id
    JOIN channels c ON c.channel_id = o.channel_id
    GROUP BY o.channel_id, c.channel_name
),
total_revenue AS (
    SELECT SUM(channel_revenue) AS total_revenue
    FROM channel_revenue
)
SELECT 
    cr.channel_id,
    cr.channel_name,
    cr.channel_revenue,
    ROUND(cr.channel_revenue / tr.total_revenue * 100, 2) AS revenue_share_percentage
FROM channel_revenue cr
CROSS JOIN total_revenue tr
ORDER BY cr.channel_revenue DESC;


-- ==========================================
-- Query 4: Revenue Share by Payment Method
-- ==========================================
-- Calculate what percentage of total revenue each payment method generates

WITH payment_revenue AS (
    SELECT 
        p.payment_method,
        SUM(p.payment_amount - p.payment_fee) AS method_revenue,
        COUNT(*) AS transaction_count
    FROM payments p
    GROUP BY p.payment_method
),
total_revenue AS (
    SELECT SUM(method_revenue) AS total_revenue
    FROM payment_revenue
)
SELECT 
    pr.payment_method,
    pr.transaction_count,
    pr.method_revenue,
    ROUND(pr.method_revenue / tr.total_revenue * 100, 2) AS revenue_share_percentage
FROM payment_revenue pr
CROSS JOIN total_revenue tr
ORDER BY pr.method_revenue DESC;


-- ==========================================
-- Query 5: Average Transaction Value by Channel
-- ==========================================
-- Compare average order value across different channels

SELECT 
    o.channel_id,
    c.channel_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(p.payment_amount) AS total_payment_amount,
    ROUND(AVG(p.payment_amount), 2) AS avg_payment_per_order,
    ROUND(AVG(p.payment_fee), 2) AS avg_fee_per_order,
    ROUND(AVG(p.payment_amount - p.payment_fee), 2) AS avg_revenue_per_order
FROM orders o
JOIN payments p ON p.payment_order_id = o.order_id
JOIN channels c ON c.channel_id = o.channel_id
GROUP BY o.channel_id, c.channel_name
ORDER BY AVG(p.payment_amount - p.payment_fee) DESC;


-- ==========================================
-- Query 6: Payment Method Distribution by Channel
-- ==========================================
-- Understand which payment methods are most popular in each channel

WITH channel_payment_counts AS (
    SELECT 
        o.channel_id,
        c.channel_name,
        p.payment_method,
        COUNT(*) AS payment_count
    FROM orders o
    JOIN payments p ON p.payment_order_id = o.order_id
    JOIN channels c ON c.channel_id = o.channel_id
    GROUP BY o.channel_id, c.channel_name, p.payment_method
),
channel_totals AS (
    SELECT 
        channel_id,
        SUM(payment_count) AS total_payments_in_channel
    FROM channel_payment_counts
    GROUP BY channel_id
)
SELECT 
    cpc.channel_id,
    cpc.channel_name,
    cpc.payment_method,
    cpc.payment_count,
    ROUND(cpc.payment_count::NUMERIC / ct.total_payments_in_channel * 100, 2) AS payment_share_percentage
FROM channel_payment_counts cpc
JOIN channel_totals ct ON ct.channel_id = cpc.channel_id
ORDER BY cpc.channel_id, cpc.payment_count DESC;


-- ==========================================
-- Query 7: Low-Performing Combinations
-- ==========================================
-- Identify channel + payment method combinations with low revenue
-- (candidates for optimization or consolidation)

WITH combination_revenue AS (
    SELECT 
        o.channel_id,
        c.channel_name,
        p.payment_method,
        COUNT(*) AS transaction_count,
        SUM(p.payment_amount - p.payment_fee) AS total_revenue
    FROM orders o
    JOIN payments p ON p.payment_order_id = o.order_id
    JOIN channels c ON c.channel_id = o.channel_id
    GROUP BY o.channel_id, c.channel_name, p.payment_method
)
SELECT 
    channel_id,
    channel_name,
    payment_method,
    transaction_count,
    total_revenue
FROM combination_revenue
WHERE total_revenue < 1000  -- Low revenue threshold
ORDER BY total_revenue ASC;


-- ==========================================
-- Query 8: Payment Fee Analysis
-- ==========================================
-- Calculate fee burden for each channel to identify cost optimization opportunities

SELECT 
    o.channel_id,
    c.channel_name,
    COUNT(*) AS total_transactions,
    SUM(p.payment_amount) AS total_payment_amount,
    SUM(p.payment_fee) AS total_fees,
    SUM(p.payment_amount - p.payment_fee) AS net_revenue,
    ROUND((SUM(p.payment_fee) / SUM(p.payment_amount)) * 100, 2) AS fee_percentage
FROM orders o
JOIN payments p ON p.payment_order_id = o.order_id
JOIN channels c ON c.channel_id = o.channel_id
GROUP BY o.channel_id, c.channel_name
ORDER BY (SUM(p.payment_fee) / SUM(p.payment_amount)) DESC;


-- ==========================================
-- Query 9: Channel Performance Ranking
-- ==========================================
-- Rank channels by multiple metrics (revenue, transaction count, avg value)

SELECT 
    o.channel_id,
    c.channel_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(p.payment_amount - p.payment_fee) AS total_revenue,
    ROUND(AVG(p.payment_amount - p.payment_fee), 2) AS avg_revenue_per_order,
    RANK() OVER (ORDER BY SUM(p.payment_amount - p.payment_fee) DESC) AS revenue_rank,
    RANK() OVER (ORDER BY COUNT(DISTINCT o.order_id) DESC) AS volume_rank,
    RANK() OVER (ORDER BY AVG(p.payment_amount - p.payment_fee) DESC) AS avg_value_rank
FROM orders o
JOIN payments p ON p.payment_order_id = o.order_id
JOIN channels c ON c.channel_id = o.channel_id
GROUP BY o.channel_id, c.channel_name
ORDER BY SUM(p.payment_amount - p.payment_fee) DESC;


-- ==========================================
-- Query 10: Payment Method Performance Ranking
-- ==========================================
-- Rank payment methods by revenue and efficiency metrics

SELECT 
    p.payment_method,
    COUNT(*) AS total_transactions,
    SUM(p.payment_amount) AS total_payment_amount,
    SUM(p.payment_fee) AS total_fees,
    SUM(p.payment_amount - p.payment_fee) AS net_revenue,
    ROUND(AVG(p.payment_amount), 2) AS avg_payment_amount,
    ROUND(AVG(p.payment_fee), 2) AS avg_fee,
    ROUND((SUM(p.payment_fee) / SUM(p.payment_amount)) * 100, 2) AS fee_percentage,
    RANK() OVER (ORDER BY SUM(p.payment_amount - p.payment_fee) DESC) AS revenue_rank
FROM payments p
GROUP BY p.payment_method
ORDER BY SUM(p.payment_amount - p.payment_fee) DESC;
