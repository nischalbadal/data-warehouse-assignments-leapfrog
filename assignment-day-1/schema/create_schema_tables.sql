create schema  employee;

create table employee.employee(
    employee_id INT PRIMARY KEY ,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL ,
    employee_role VARCHAR(100) NOT NULL,
    hire_date DATE NOT NULL,
    terminated_date DATE default '01-01-1700',
    terminated_reason VARCHAR(500),
    dob DATE NOT NULL ,
    fte FLOAT NOT NULL,
    location VARCHAR(200)
);