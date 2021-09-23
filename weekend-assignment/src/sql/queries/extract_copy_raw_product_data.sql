INSERT INTO copy_raw_product( product_id,product_name,description,price,mrp,pieces_per_case,weight_per_piece,uom,
brand,category,tax_percent,active,created_by,created_date,updated_by,updated_date)
SELECT
        product_id,
        product_name,
        description,
        price,
        mrp,
        pieces_per_case,
        weight_per_piece,
        uom,
        brand,
        category,
        tax_percent,
        active,
        created_by,
        created_date,
        updated_by,
        updated_date
FROM raw_product;