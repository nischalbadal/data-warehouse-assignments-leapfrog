INSERT INTO fact_product(product_id, product_name, description, price, mrp, pieces_per_case, weight_per_peice, uom_id, brand_id,
                         category_id, tax_percent, active_status_id, created_by, created_date, updated_by, updated_date)
SELECT
CAST(product_id AS INT) AS product_id,
product_name AS product_name,
description AS description,
CAST(price AS FLOAT) AS price,
CAST(mrp AS FLOAT) AS mrp,
CAST(pieces_per_case AS INT) AS pieces_per_case,
CAST(weight_per_piece AS FLOAT) AS weight_per_piece,
u.id as uom_id,
b.id AS brand_id,
c.id AS category_id,
CAST(tax_percent AS FLOAT) AS tax_percent,
a.id AS active_status_id,
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
FROM raw_product p
INNER JOIN dim_uom u on p.uom = u.uom_name
INNER JOIN dim_brand b on p.brand = b.brand_name
INNER JOIN dim_category c on INITCAP(p.category) = c.category_name
INNER JOIN dim_active_status a on p.active = a.status;