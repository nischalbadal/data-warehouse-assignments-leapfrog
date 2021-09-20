create table fact_employee(
    employee_id SERIAL PRIMARY KEY,
    client_employee_id VARCHAR(250),
    department_id INT,
    manager_id INT,
    role_id INT,
    salary FLOAT,
    active_status_id INT,
    weekly_hours FLOAT,
    CONSTRAINT fk_department_id FOREIGN KEY (department_id) REFERENCES dim_department(id),
    CONSTRAINT fk_manager_id FOREIGN KEY (manager_id) REFERENCES dim_manager(id),
    CONSTRAINT fk_role_id FOREIGN KEY (role_id) REFERENCES dim_role(id),
    CONSTRAINT fk_active_status_id FOREIGN KEY (active_status_id) REFERENCES dim_status(status_id)
);

