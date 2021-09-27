INSERT INTO copy_raw_customer(customer_id,user_name,first_name,last_name,country,town,active)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (customer_id)
DO UPDATE SET
       customer_id = EXCLUDED.customer_id,
       user_name = EXCLUDED.user_name,
       first_name = EXCLUDED.first_name,
       last_name = EXCLUDED.last_name,
       country = EXCLUDED.country,
       town = EXCLUDED.town,
       active = EXCLUDED.active;

       