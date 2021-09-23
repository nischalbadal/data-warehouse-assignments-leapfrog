INSERT INTO dim_brand(brand_name)
SELECT DISTINCT(brand) FROM raw_product;