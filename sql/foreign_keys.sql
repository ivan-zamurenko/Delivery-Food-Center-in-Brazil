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
