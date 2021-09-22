insert into fact_timesheet(employee_id, work_date, department_id, hours_worked, shift_type_id, punch_in_time, punch_out_time, time_period_id, attendance, has_taken_break, break_hour, was_charge, charge_hour, was_on_call, on_call_hour, is_weekend, num_teammates_absent)
select
    e.id,
    shift_date as work_date,
    (select id from dim_department where client_department_id = t.department_id) as department_id,
    hours_worked,
    s.id as shift_type_id,
    shift_start_time as punch_in_time,
    shift_end_time as punch_out_time,
    (select id from dim_period p) as time_period_id,
    attendance,
    has_taken_break,
    break_hour,
    was_charge,
    charge_hour,
    was_on_call,
    on_call_hour,
    (case when EXTRACT(ISODOW FROM shift_date) IN (6, 7) then true else false end) as is_weekend,
    num_teammates_absent
from timesheet t
    inner join employee e on e.client_employee_id = t.employee_id
    left join dim_shift_type s on s.name = t.shift_type;

    