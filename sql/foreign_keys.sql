--!> Add Foreign Keys
--?> Before Adding FKs: Run Checks

--!> 1. Check for non-matching foreign key values
    --?> 1.1 Check for non-matching store_id in orders table
    Select o.store_id
    From orders o
    Where store_id Not In (Select store_id From stores s);

    --?> Create a foreign key for orders(store_id) -> stores(store_id)
    Alter Table orders
    Add Constraint fk_orders_stores
    Foreign Key (store_id) References stores(store_id);


    --?> 1.2 Check for non-matching channel_id in orders table
    Select o.channel_id
    From orders o
    Left Join channels c On c.channel_id = o.channel_id
    Where c.channel_id Is Null;

    --?> Create a foreign key for orders(channel_id) -> channels(channel_id)
    Alter Table orders
    Add constraint fk_orders_channels
    Foreign Key (channel_id) References channels(channel_id);