create table ecom.fact_sale_product(
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL references ecom.fact_product(product_id),
    time_period_id INT NOT NULL references ecom.dim_period(id),
    total_quantity_sold FLOAT NOT NULL ,
    unit_price FLOAT NOT NULL ,
    total_gross_price FLOAT NOT NULL,
    total_tax_price FLOAT NOT NULl
);

