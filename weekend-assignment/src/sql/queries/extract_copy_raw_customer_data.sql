INSERT INTO copy_raw_customer(customer_id,user_name,first_name,last_name,country,town,active)
SELECT
       customer_id,
       user_name,
       first_name,
       last_name,
       country,
       town,
       active
FROM raw_customer;