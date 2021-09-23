create table fact_sale_product(
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL references fact_product(product_id),
    time_period_id INT NOT NULL references dim_period(id),
    total_quantity_sold FLOAT NOT NULL ,
    avg_unit_price FLOAT NOT NULL ,
    total_gross_price FLOAT NOT NULL,
    total_discount_amount FLOAT NOT NULL,
    total_tax_amount FLOAT NOT NULL,
    total_net_bill_amount FLOAT NOT NULL,
    total_customers INT NOT NULL
);



