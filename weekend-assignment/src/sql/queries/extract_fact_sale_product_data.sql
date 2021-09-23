INSERT INTO fact_sale_product(product_id, time_period_id, total_quantity_sold, avg_unit_price, total_gross_price, total_discount_amount,
                              total_tax_amount, total_net_bill_amount, total_customers)
SELECT
    product_id,
    (SELECT time_period_id FROM get_dim_period_id_view v
    WHERE v.bill_date = s.bill_date
        ) as time_period_id,
    SUM(CAST(quantity AS INT)) AS total_quantity_sold,
	ROUND(AVG(CAST(price AS FLOAT))::NUMERIC,2) AS avg_unit_price,
	ROUND(SUM(CAST(gross_price AS FLOAT))::NUMERIC,2) AS total_gross_price,
	SUM(CAST(discount_amount AS FLOAT)) AS total_discount_amount,
	ROUND(sum(CAST(tax_amount AS FLOAT))::NUMERIC,2) AS total_tax_amount,
	ROUND(sum(CAST(net_bill_amt AS FLOAT))::NUMERIC,2) AS total_net_bill_amount,
    COUNT(customer_id) AS total_customers
FROM fact_sale s
GROUP BY product_id, s.bill_date;

