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