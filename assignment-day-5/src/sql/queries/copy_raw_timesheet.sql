insert into copy_raw_timesheet(employee_id, cost_center, punch_in_time, punch_out_time, punch_apply_date, hours_worked, paycode)
select * from raw_timesheet;
