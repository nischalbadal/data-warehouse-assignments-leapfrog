insert into dim_department(client_department_id, name)
select client_department_id, department_name from department;

