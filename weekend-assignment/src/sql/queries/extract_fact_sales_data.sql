INSERT INTO fact_sale(id, transaction_id, bill_no, bill_date, bill_loaction, customer_id, product_id, quantity, uom_id, price,
                      gross_price, tax_pc, tax_amount, discount_pc, discount_amount,  created_by, created_date,updated_by,updated_date)
SELECT
       CAST(s.id AS INT) AS id,
       CAST(transaction_id AS INT) as transaction_id,
       CAST(bill_no AS INT) as bill_no,
       CASE
           WHEN bill_date = '2017-02-30 11:00:00' THEN TO_TIMESTAMP('2017-02-28 11:00:00', 'YYYY-MM-DD HH24:MI:SS')
           ELSE TO_TIMESTAMP(bill_date, 'YYYY-MM-DD HH24:MI:SS')
           END AS bill_date,
       bill_location,
       CAST(customer_id AS INT) AS customer_id,
       CAST(product_id AS INT) AS product_id,
       CAST(qty AS INT) AS quantity,
       u.id AS uom_id,
       CAST(price AS FLOAT) AS price,
       CAST(gross_price AS FLOAT) AS gross_price,
       CAST(tax_pc AS FLOAT) AS tax_pc,
       CAST(tax_amt AS FLOAT) AS tax_amountt,
       CAST(discount_pc AS FLOAT) AS discount_pc,
       CAST(discount_amt AS FLOAT) AS discount_amt,
       INITCAP(created_by) AS created_by,
       CASE
           WHEN created_date = '2017-02-30 11:00:00'
               THEN TO_TIMESTAMP('2017-02-28 11:00:00', 'YYYY-MM-DD HH24:MI:SS')
           ELSE TO_TIMESTAMP(created_date, 'YYYY-MM-DD HH24:MI:SS')
           END AS created_date,
        INITCAP(updated_by) AS updated_by,
       CASE
           WHEN updated_date = '2017-02-30 11:00:00'
               THEN TO_TIMESTAMP('2017-02-28 11:00:00', 'YYYY-MM-DD HH24:MI:SS')
           WHEN updated_date = '-' THEN NULL
           ELSE TO_TIMESTAMP(updated_date, 'YYYY-MM-DD HH24:MI:SS')
           END AS updated_date
FROM raw_sales s
INNER JOIN dim_uom u
ON s.uom = u.uom_name
WHERE CAST(s.qty AS INT) <> 0;
