# Requirements Gathering and Modeling for eCommerce
The following is the requirements gathering, conceptual model and physical model of the data warehouse to analyze the sales.

## Requirements Identification
The following requirements were identified:
- To find out the trends among the customers
- To analyze their sales to invest more on products that are selling fast 
- To invest more on high profit products
- To analyze the sales and remove products that are not selling so good

## Potential area of analysis
- sales
- products
- product brands
- customer

## Conceptual Data Modeling

To model the data in a data warehouse, we identify the dimensions table and facts table. We then build a conceptual model using different design schemas. All the steps of conceptual data modeling of the data warehouse are briefly explained hereafter.

#### 1. Identifying Dimensions Table
The following are the dimension tables identified for the data warehouse to analyze the sales:
```dim_uom``` - Dimension table containing different units of measures
```dim_brand``` - Dimension table containing brand information of products
```dim_category``` - Dimension table containing category of products
```dim_customer``` - Dimension table consisting information of customers
```dim_active_status``` - Dimension table containing the active status of products
```dim_period``` - Dimension table having two dates for analysis of sales

#### 2. Identifying Facts Table
Similarly after identifying the dimension tables, we now identify the fact tables to create the data warehouse and carry out the analysis. The fact tables identified are:
```fact_sale``` - Consists all the records of sales of products 
```fact_sale_product``` - Used for the summerization of sale of any specific product in a certain time frame of dim_period
```fact_product``` - Used as the description of products 

#### 3. Identifying the attributes of facts and dimension tables
This step occurs in the logical design modeling phase. But it can be done in conceptual modeling as well. Here, we identify all tha attributes of the entities pre-sepcified (in number 1 and 2 above).

The attributes of the fact and dimension tables can be identified as:

| Table Name | Attributes | 
| :---: | :---: | 
| ```dim_uom``` | id, uom_name | 
| ```dim_brand``` | id, brand_name | 
| ```dim_category``` | id, category_name | 
| ```dim_active_status``` | id, status | 
| ```dim_period``` | id, start_date, end_date | 
| ```fact_product``` | product_id, product_name, description, price, mrp, peices_per_case, weight_per_peice, uom_id, brand_id, category_id, tax_percent, active_status_id, created_by, created_date, updated_by, updated_date | 
|```fact_sale_product```|id, product_id, time_period_id, total_quantity_sold, avg_unit_price, total_gross_price, total_discount_amount, total_tax_amount, total_net_bill_amount,total_customers|
|```fact_sale```|id, transaction_id, bill_no, bill_date, bill_location, customer_id, product_id, quantity, uom_id, price, gross_price, tax_pc, tax_amount, discount_pc, discount_amount, net_bill_amt, created_by, created_date, updated_by, updated_date|

#### 4. ER Diagram
After we identify the tables abd their attributes, we now create an ER diagram for the data warehouse. Since, we have multiple facts table, our ER diagram would be of Fact Constellation Schema design which incorporated multiple facts table.

The proposed ER Diagram of the data warehouse would be:

![Proposed ER Diagram](data-warehouse-design.drawio.png)

## Physical Modeling

After conceptual modeling of the system, we now build the physical modeling (or implementation) of the data warehouse. Physical Modeling is done using the postgresql database. All the DDL queries of the physical model of the data warehouse are listed below:

Creating Main schema for data warehouse tables:
```sql
```
Creating Dimension Tables:
```sql
create table dim_active_status(
  id SERIAL PRIMARY KEY,
  status CHAR NOT NULL
);
```

```sql
create table dim_brand(
    id SERIAL PRIMARY KEY,
    brand_name VARCHAR(250) NOT NULL
);
```
```sql
create table dim_category(
    id SERIAL PRIMARY KEY,
    category_name VARCHAR(250) NOT NULL
);
```
```sql
create table dim_customer(
    customer_id INT PRIMARY KEY,
    user_name VARCHAR(250) UNIQUE NOT NULL,
    first_name VARCHAR(250) NOT NULL,
    last_name VARCHAR(250) NOT NULL,
    country VARCHAR(250) NOT NULL,
    town VARCHAR(250) NOT NULL,
    active BOOLEAN NOT NULL
);
```
```sql
create table dim_period(
   id SERIAL PRIMARY KEY,
   start_date DATE NOT NULL,
   end_date DATE NOT NULL
);
```
```sql
create table dim_uom(
    id SERIAL PRIMARY KEY,
    uom_name VARCHAR(250) NOT NULL
);
```
Similarly creating Facts Table: 
```sql
create table fact_product(
    product_id INT PRIMARY KEY,
    product_name VARCHAR(250) NOT NULL,
    description VARCHAR(250) ,
    price FLOAT NOT NULL,
    mrp FLOAT NOT NULL,
    pieces_per_case FLOAT NOT NULL,
    weight_per_peice FLOAT NOT NULL,
    uom_id INT NOT NULL references dim_uom(id),
    brand_id INT NOT NULL references dim_brand(id),
    category_id INT NOT NULL references dim_category(id),
    tax_percent FLOAT NOT NULL,
    active_status_id INT NOT NULL references dim_active_status(id),
    created_by VARCHAR(250),
    created_time TIMESTAMP,
    updated_by VARCHAR(250),
    updated_time TIMESTAMP
);
```
```sql
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
```
```sql
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
```

Hence, this document explains the requirements gathering, conceptual model and physical implementation of sales data warehouse.