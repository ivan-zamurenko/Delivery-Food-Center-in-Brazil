--?> Preview the first 10 rows of the tables
Select * From orders Limit 10;
Select * From payments Limit 10;
Select * From deliveries Limit 10;
Select * From stores Limit 10;
Select * From channels Limit 10;
Select * From drivers Limit 10;
Select * From hubs Limit 10;


--!> Q1: What is the total number of orders placed in each store?
Select 
    store_id,
    count(order_id) as total_orders
From orders
Group By store_id
Order By count(order_id) DESC


--!> Q2: What is the average payment_amount & payment_fee for each payment method?
Select
    payment_method,
    round(avg(payment_amount), 2) as average_payment,
    round(avg(payment_fee), 2) as average_fee
From payments
Group By payment_method
Order By avg(payment_amount) DESC


--!> Q3: Which delivery driver has completed the most deliveries?
With top_performers as (
    Select 
        driver_id,
        count(delivery_id) as delivered_orders
    From deliveries
    Where upper(delivery_status) = 'DELIVERED'
    Group By driver_id
    Order By count(delivery_id) DESC
    Limit 1
)
Select
    d.driver_id,
    d.driver_modal,
    d.driver_type,
    tp.delivered_orders
From top_performers tp
Join drivers d On d.driver_id = tp.driver_id


--!> Q4: What is the total revenue generated from each sales channel?
With revenue_per_channel as (
    Select
        channel_id,
        count(order_amount) as revenue_per_channel
    From orders
    Group By channel_id
    Order By channel_id
)
Select
    -- c.channel_id,
    c.channel_name,
    -- c.channel_type,
    rpc.revenue_per_channel
From revenue_per_channel rpc
Join channels c On c.channel_id = rpc.channel_id
Order By rpc.revenue_per_channel DESC


--!> Q5: Which hub has the highest number of associated stores?
With stores_per_hub as (
    Select 
        hub_id,
        count(store_id) as total_stores
    From stores
    Group By hub_id
    Order By count(store_id) DESC
    Limit 5
)
Select 
    h.hub_id,
    h.hub_name,
    h.hub_city, 
    sph.total_stores
From stores_per_hub sph
Join hubs h On h.hub_id = sph.hub_id


--!> Q6: What is the average delivery time for each driver type?
--?> Find the average delivery time for 'DELIVERED' orders
With success_deliveries as (
    Select 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id
    From orders o
    Join deliveries d On d.delivery_order_id = o.order_id
    Where upper(d.delivery_status) = 'DELIVERED'
        and o.order_moment_delivering IS NOT NULL
        and o.order_moment_delivered IS NOT NULL
)
Select 
    d.driver_id,
    d.driver_modal,
    d.driver_type,
    round(avg(extract(epoch from (sd.order_moment_delivered - sd.order_moment_delivering)) / 60), 2) as average_delivery_time_minutes
From success_deliveries sd
Join drivers d On d.driver_id = sd.driver_id
Group By d.driver_id, d.driver_modal, d.driver_type
Order By average_delivery_time_minutes ASC;


--!> Q7: What is the total number of orders and total revenue for each store?
Select 
    store_id
    , count(order_id) as total_orders
    , sum(p.payment_amount) as total_revenue
From orders
Join payments p On p.payment_order_id = orders.order_id
Group By store_id
Order By sum(p.payment_amount) DESC;


--!> Q8: Which payment method has the highest average payment amount?
with avg_payments as (
    Select 
        payment_method,
        round(avg(payment_amount), 2) as average_payment_amount,
        row_number() over (Order By avg(payment_amount) DESC) as rank
    From payments
    Group By payment_method
)
Select *
From avg_payments
Where rank = 1;


--!> Q9: What is the distribution of orders across different sales channels?
Select 
    o.channel_id,
    c.channel_name,
    count(o.order_id) as total_orders,
    round(count(o.order_id) / sum(count(o.order_id)) over() * 100, 5) as percentage_of_total_orders
From orders o
Join channels c On c.channel_id = o.channel_id
Group By 
    o.channel_id,
    c.channel_name
Order By count(o.order_id) DESC;


--!> Task A: Identify the most profitable channels + payment methods
Select 
        o.channel_id,
        p.payment_method,
        count(*) as payments_count,
        sum(p.payment_amount - p.payment_fee) as total_revenue
From orders o
Join payments p On p.payment_order_id = o.order_id
Group By o.channel_id, p.payment_method
Order By sum(p.payment_amount - p.payment_fee) DESC


--!> Task B: Delivery Time Optimization (Driver Analysis)
With delivered_orders as (
    Select 
        o.order_id,
        o.order_moment_delivering,
        o.order_moment_delivered,
        d.driver_id,
        d.delivery_status
    From orders o
    Join deliveries d On d.delivery_order_id = o.order_id
    Where 
        upper(d.delivery_status) = 'DELIVERED'
        and o.order_moment_delivering IS NOT NULL
        and o.order_moment_delivered IS NOT NULL
)
Select
    d.driver_modal,
    count(*) as total_orders,
    round(avg(extract(epoch from (del.order_moment_delivered - del.order_moment_delivering)) / 60), 2) as average_delivery_time__minutes
From delivered_orders del
Join drivers d On d.driver_id = del.driver_id
Group By d.driver_modal
Order By average_delivery_time__minutes ASC;