CREATE TABLE fact_timesheet(
    employee_id  INT NOT NULL,
    work_date DATE NOT NULL,
    department_id INT NOT NULL,
    hours_worked FLOAT NOT NULL,
    shift_type_id VARCHAR(255) NOT NULL,
    punch_in_time TIMESTAMP NOT NULL,
    punch_out_time TIMESTAMP NOT NULL,
    time_period_id INT NOT NULL,
    attendance VARCHAR(100) NOT NULL,
    has_taken_break BOOLEAN NOT NULL,
    break_hour FLOAT NOT NULL,
    was_charge BOOLEAN NOT NULL,
    charge_hour FLOAT NOT NULL,
    was_on_call BOOLEAN NOT NULL,
    on_call_hour FLOAT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    num_teammates_absent INT NOT NULL,
    CONSTRAINT fk_department_id FOREIGN KEY (department_id) REFERENCES dim_department(id),
    CONSTRAINT fk_employee_id FOREIGN KEY (employee_id) REFERENCES fact_employee (employee_id)
);

