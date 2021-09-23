INSERT INTO dim_customer (customer_id, user_name, first_name, last_name, country, town, active)
SELECT
cast(customer_id AS INT) as customer_id,
user_name AS user_name,
first_name AS first_name,
regexp_replace(last_name, '[^\w]+_','') AS last_name,
INITCAP(country) as country,
town,
CASE WHEN active = 'Y' THEN true ELSE false end as active
FROM raw_customer rc;

