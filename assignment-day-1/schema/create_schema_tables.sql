create schema  employee;

create table employee.department(
  department_id INT PRIMARY KEY,
  department_name VARCHAR(200)
);

create table employee.employee(
    employee_id INT PRIMARY KEY ,
    department_id INT NOT NULL,
    manager_employee_id INT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL ,
    employee_role VARCHAR(100) NOT NULL,
    hire_date DATE NOT NULL,
    terminated_date DATE default '01-01-1700',
    terminated_reason VARCHAR(500),
    salary FLOAT NOT NULL,
    dob DATE NOT NULL ,
    fte FLOAT NOT NULL,
    location VARCHAR(200),
    constraint fk_department_id
        FOREIGN KEY (department_id)
                REFERENCES employee.department(department_id),
    constraint fk_manager_employee_id
        FOREIGN KEY (manager_employee_id)
                REFERENCES employee.employee(employee_id)
);

create table employee.punch_date(
    id SERIAL PRIMARY KEY,
    punch_apply_date DATE NOT NULL,
    day_of_week VARCHAR(100)

);
create table employee.employee_attendance(
  attendance_id SERIAL PRIMARY KEY,
  employee_id INT NOT NULL,
  cost_center INT NOT NULL,
  punch_in_time TIMESTAMP,
  punch_out_time TIMESTAMP,
  working_shift VARCHAR(200),
  work_hour FLOAT NOT NULL,
  punch_apply_date INT NOT NULL,
  paycode VARCHAR(10) NOT NULL,
  constraint fk_employee_id
        FOREIGN KEY (employee_id)
                REFERENCES employee.employee(employee_id),
  constraint fk_punch_apply_date
        FOREIGN KEY (punch_apply_date)
                REFERENCES employee.punch_date(id)
);
