INSERT INTO dim_category(category_name)
SELECT DISTINCT(INITCAP(category)) FROM raw_product;

