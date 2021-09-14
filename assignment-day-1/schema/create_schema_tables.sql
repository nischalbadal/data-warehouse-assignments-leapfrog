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

create table employee.department(
  department_id INT PRIMARY KEY,
  department_name VARCHAR(200),
  manager_employee_id INT,
  constraint fk_manager_employee_id
        FOREIGN KEY (manager_employee_id)
                REFERENCES employee.employee(employee_id)
);
drop table employee.timesheet;

create table employee.timesheet(
  timesheet_id SERIAL PRIMARY KEY,
  employee_id INT NOT NULL,
  cost_center INT NOT NULL,
  punch_in_time TIMESTAMP default now(),
  punch_out_time TIMESTAMP default now(),
  punch_apply_date DATE default current_date NOT NULL ,
  paycode VARCHAR(10) NOT NULL,
  constraint fk_employee_id
        FOREIGN KEY (employee_id)
                REFERENCES employee.employee(employee_id),
  constraint fk_cost_center
        FOREIGN KEY (cost_center)
                REFERENCES employee.department(department_id)
);

create table employee.employee_records(
    record_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL,
    timesheet_id INT NOT NULL,
    department_id INT NOT NULL,
    shift_name VARCHAR(200),
    salary FLOAT NOT NULL,
    work_hour FLOAT NOT NULL,
    constraint fk_employee_id
        FOREIGN KEY (employee_id)
                REFERENCES employee.employee(employee_id),
    constraint fk_timesheet_id
        FOREIGN KEY (timesheet_id)
                REFERENCES employee.timesheet(timesheet_id),
    constraint fk_department_id
        FOREIGN KEY (department_id)
                REFERENCES employee.department(department_id)
);