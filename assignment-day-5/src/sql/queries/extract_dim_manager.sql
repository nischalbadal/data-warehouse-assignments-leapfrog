insert into dim_manager(client_employee_id, first_name, last_name)
select distinct
m.client_employee_id, m.first_name, m.last_name
from employee e
inner join employee m on e.manager_emp_id = m.client_employee_id;

