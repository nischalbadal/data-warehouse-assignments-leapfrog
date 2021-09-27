INSERT INTO copy_raw_product( product_id,product_name,description,price,mrp,pieces_per_case,weight_per_piece,uom,
brand,category,tax_percent,active,created_by,created_date,updated_by,updated_date)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(product_id) 
DO UPDATE SET 
        description = EXCLUDED.description, 
        price = EXCLUDED.price,  
        mrp = EXCLUDED.mrp, 
        pieces_per_case = EXCLUDED.pieces_per_case, 
        weight_per_piece = EXCLUDED.weight_per_piece, 
        uom = EXCLUDED.uom, 
        brand = EXCLUDED.brand, 
        category = EXCLUDED.category, 
        tax_percent = EXCLUDED.tax_percent, 
        active = EXCLUDED.active, 
        created_by = EXCLUDED.created_by, 
        created_date = EXCLUDED.created_date, 
        updated_by = EXCLUDED.updated_by, 
        updated_date = EXCLUDED.updated_date;

        