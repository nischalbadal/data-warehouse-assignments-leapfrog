INSERT INTO dim_active_status(status)
select distinct active from raw_product;

