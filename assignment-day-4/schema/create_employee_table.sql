create table employee(
    id serial primary key,
    client_employee_id varchar(200) unique,
    department_id varchar(200),
    first_name varchar(200),
    last_name varchar(200),
    manager_emp_id varchar(200),
    salary float,
    hire_date date,
    term_date date,
    term_reason varchar(500),
    dob date,
    fte float,
    weekly_hours float,
    role varchar(200)
);

