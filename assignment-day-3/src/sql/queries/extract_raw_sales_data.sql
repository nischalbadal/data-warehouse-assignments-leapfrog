select s.user_id, u.username, s.product_id, p.name as product_name, p.category_id,
       c.name as category_name, p.price as current_price, s.price as sold_price, s.quantity as sold_quantity,
       (p.quantity-s.quantity) as remaining_quantity,s.updated_at
from sales s
inner join users u ON u.id = s.user_id
inner join products p ON p.id = s.product_id
inner join categories c ON c.id = p.category_id;