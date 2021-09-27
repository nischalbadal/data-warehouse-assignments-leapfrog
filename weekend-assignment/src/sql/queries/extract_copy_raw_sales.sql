INSERT INTO copy_raw_sales( id,transaction_id,bill_no,bill_date,bill_location,customer_id,product_id,qty,uom,
                       price,gross_price,tax_pc,tax_amt,discount_pc,discount_amt,net_bill_amt,created_by,
                       updated_by,created_date,updated_date)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(id) 
DO UPDATE SET 
    id = EXCLUDED.id,
    transaction_id = EXCLUDED.transaction_id,
    bill_no = EXCLUDED.bill_no,
    bill_date = EXCLUDED.bill_date,
    bill_location = EXCLUDED.bill_location,
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    qty = EXCLUDED.qty,
    uom = EXCLUDED.uom,
    price = EXCLUDED.price,
    gross_price = EXCLUDED.gross_price,
    tax_pc = EXCLUDED.tax_pc,
    tax_amt = EXCLUDED.tax_amt,
    discount_pc = EXCLUDED.discount_pc,
    discount_amt = EXCLUDED.discount_amt,
    net_bill_amt = EXCLUDED.net_bill_amt,
    created_by = EXCLUDED.created_by,
    updated_by = EXCLUDED.updated_by,
    created_date = EXCLUDED.created_date,
    updated_date = EXCLUDED.updated_date;

    