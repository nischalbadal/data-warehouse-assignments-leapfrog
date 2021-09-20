create table ecom.fact_product(
    product_id INT PRIMARY KEY,
    product_name VARCHAR(250) NOT NULL,
    description VARCHAR(250) ,
    price FLOAT NOT NULL,
    mrp FLOAT NOT NULL,
    pieces_per_case FLOAT NOT NULL,
    weight_per_case FLOAT NOT NULL,
    uom_id INT NOT NULL references ecom.dim_uom(id),
    brand_id INT NOT NULL references ecom.dim_brand(id),
    category_id INT NOT NULL references ecom.dim_category(id),
    tax_percent FLOAT NOT NULL,
    active_status_id INT NOT NULL references ecom.dim_active_status(id),
    created_by VARCHAR(250),
    created_at TIMESTAMP,
    updated_by VARCHAR(250),
    updated_at TIMESTAMP
);

