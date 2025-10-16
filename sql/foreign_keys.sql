--!> Add Foreign Keys
--?> Before Adding FKs: Run Checks

--!> 1. Check for non-matching foreign key values for ORDERS table
    --?> 1.1 Check for non-matching store_id in ORDERS table
    Select o.store_id
    From orders o
    Where store_id Not In (Select store_id From stores s);


    --?> Create a foreign key for orders(store_id) -> stores(store_id)
    Alter Table orders
    Add Constraint fk_orders_stores
    Foreign Key (store_id) References stores(store_id);


    --?> 1.2 Check for non-matching channel_id in ORDERS table
    Select o.channel_id
    From orders o
    Left Join channels c On c.channel_id = o.channel_id
    Where c.channel_id Is Null;

    --?> Create a foreign key for orders(channel_id) -> channels(channel_id)
    Alter Table orders
    Add constraint fk_orders_channels
    Foreign Key (channel_id) References channels(channel_id)


--!> 2. Check for non-matching foreign key values for DELIVERIES table
    --?> 2.1 Check for non-matching delivery_order_id in DELIVERIES table
    Select d.delivery_order_id
    From deliveries d
    Left Join orders o On o.order_id = d.delivery_order_id
    Where o.order_id Is Null;

    --?> Create a foreign key for deliveries(delivery_order_id) -> orders(order_id)
    Alter Table deliveries
    Add Constraint fk_deliveries_orders
    Foreign Key (delivery_order_id) References orders(order_id);


--!> 3. Check for non-matching foreign key values for PAYMENTS table
    --?> 3.1 Check for non-matching payment_order_id in PAYMENTS table
    Select p.payment_order_id
    From payments p
    Left Join orders o On o.order_id = p.payment_order_id
    Where o.order_id Is Null;

    --?> Create a foreign key for payments(payment_order_id) -> orders(order_id)
    Alter Table payments
    Add constraint fk_payments_orders
    Foreign Key (payment_order_id) References orders(order_id);


--!> 4. Check for non-matching foreign key values for DELIVERIES table
    --?> 4.1 Check for non-matching driver_id in DELIVERIES table
    Select del.driver_id
    From deliveries del
    Left Join drivers d On d.driver_id = del.driver_id
    Where d.driver_id Is Null;

    --?> Create a foreign key for deliveries(driver_id) -> drivers(driver_id)
    Alter Table deliveries
    Add Constraint fk_deliveries_drivers
    Foreign Key (driver_id) References drivers(driver_id);


--!> 5. Check for non-matching foreign key values for STORES table
    --?> 5.1 Check for non-matching manager_id in STORES table
    Select *
    From stores s
    Left Join hubs h on h.hub_id = s.hub_id
    Where h.hub_id Is Null;

    --?> Create a foreign key for stores(hub_id) -> hubs(hub_id)
    Alter Table stores
    Add Constraint fk_stores_hubs
    Foreign Key (hub_id) References hubs(hub_id);