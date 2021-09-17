create table if not exists employee_shift_timings(
    employee_id varchar(250),
    shift_date date,
    shift_start_time time,
    shift_end_time time,
    shift_type varchar(250),
    hours_worked numeric(5,2)
);