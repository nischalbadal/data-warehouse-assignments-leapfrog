create table fact_sale(
    id INT NOT NULL,
    transaction_id INT NOT NULL,
    bill_no INT NOT NULL,
    bill_date DATE NOT NULL,
    bill_loaction VARCHAR(250),
    customer_id INT NOT NULL references dim_customer(customer_id),
    product_id INT NOT NULL references fact_product(product_id),
    quantity FLOAT NOT NULL,
    uom_id INT NOT NULL references dim_uom(id),
    price FLOAT NOT NULL,
    gross_price FLOAT NOT NULL,
    tax_pc FLOAT NOT NULL default 0,
    tax_amount FLOAT NOT NULL,
    discount_pc FLOAT NOT NULL default 0,
    discount_amount FLOAT NOT NULL default 0,
    net_bill_amt FLOAT GENERATED ALWAYS AS (gross_price + tax_amount) STORED,
    created_by VARCHAR(250),
    updated_by VARCHAR(250),
    created_date TIMESTAMP,
    updated_date TIMESTAMP
);

