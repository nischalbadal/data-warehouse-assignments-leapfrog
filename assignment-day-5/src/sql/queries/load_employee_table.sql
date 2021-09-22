insert into employee(client_employee_id, department_id, first_name, last_name, manager_emp_id, salary,
                     hire_date, term_date, term_reason, dob, fte, weekly_hours, role)
select
    employee_id as client_employee_id,
    d.id as department_id,
    INITCAP(first_name) as first_name,
    INITCAP(last_name) as last_name,
    (case when manager_employee_id = '-' then null else manager_employee_id end) as manager_emp_id,
    cast(salary as float) as salary,
    cast(hire_date as date) as hire_date,
    cast(case when terminated_date='01-01-1700' then null else terminated_date end as date) as term_date,
    terminated_reason as term_reason,
    cast(dob as date) as dob,
    cast(fte as float) as fte,
    cast(fte as float) * 40 as weekly_hours,
    (case when employee_role LIKE '%Mgr%' AND employee_role LIKE '%Supv%' then 'Manager' else 'Supervisor' end) as role
from raw_employee
join department d on raw_employee.department_id = d.client_department_id
where employee_id <> 'employee_id';

