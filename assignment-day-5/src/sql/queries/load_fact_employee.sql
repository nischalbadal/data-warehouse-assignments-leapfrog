insert into fact_employee(client_employee_id, department_id, manager_id, role_id, salary, active_status_id, weekly_hours)
select
    e.client_employee_id,
    cast(department_id as INT) as department_id,
    dm.id as manager_id,
    dr.id as role_id,
    e.salary,
    (select status_id from dim_status where name=(case when e.term_date is not null then 'Terminated' else 'Active' end)) as active_status_id,
    e.weekly_hours
from employee e
    left join dim_manager dm on dm.client_employee_id = e.manager_emp_id
    join dim_role dr on e.role = dr.name;

