create table copy_raw_timesheet(
    employee_id	VARCHAR(500),
    cost_center	VARCHAR(500),
    punch_in_time VARCHAR(500),
    punch_out_time VARCHAR(500),
    punch_apply_date VARCHAR(500),
    hours_worked VARCHAR(500),
    paycode VARCHAR(500)
);

create table copy_raw_employee(
    employee_id VARCHAR(500),
    first_name VARCHAR(500),
    last_name VARCHAR(500),
    department_id VARCHAR(500),
    department_name VARCHAR(500),
    manager_employee_id VARCHAR(500),
    employee_role VARCHAR(500),
    salary VARCHAR(500),
    hire_date VARCHAR(500),
    terminated_date VARCHAR(500),
    terminated_reason VARCHAR(500),
    dob VARCHAR(500),
    fte VARCHAR(500),
    location VARCHAR(500)
);