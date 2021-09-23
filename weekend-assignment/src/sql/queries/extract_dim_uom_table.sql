INSERT INTO dim_uom(uom_name)
select distinct uom from raw_product;

