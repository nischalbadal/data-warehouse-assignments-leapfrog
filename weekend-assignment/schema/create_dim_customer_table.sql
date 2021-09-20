create table ecom.dim_customer(
    customer_id INT PRIMARY KEY,
    user_name VARCHAR(250) UNIQUE NOT NULL,
    first_name VARCHAR(250) NOT NULL,
    last_name VARCHAR(250) NOT NULL,
    country VARCHAR(250) NOT NULL,
    town VARCHAR(250) NOT NULL,
    active BOOLEAN NOT NULL
);