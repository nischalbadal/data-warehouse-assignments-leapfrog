INSERT INTO timesheet(employee_id, department_id, shift_start_time, shift_end_time,shift_date, shift_type, hours_worked,attendance, has_taken_break, break_hour, was_charge, charge_hour, was_on_call, on_call_hour, num_teammates_absent)
select
       employee_id,
       cost_center as department_id,
       (select shift_start_time from employee_shift_timings s
       where t.employee_id = s.employee_id and cast(t.punch_apply_date as date) = s.shift_date),
       (select shift_end_time from employee_shift_timings s
       where t.employee_id = s.employee_id and cast(t.punch_apply_date as date) = s.shift_date),
       cast(punch_apply_date as date) as shift_date,
        (select shift_type from employee_shift_timings s
       where t.employee_id = s.employee_id and cast(t.punch_apply_date as date) = s.shift_date),
        (select hours_worked from employee_shift_timings s
       where t.employee_id = s.employee_id and cast(t.punch_apply_date as date) = s.shift_date),
       case when paycode = 'ABSENT' then 'ABSENT'
       else 'Present' end as attendance,
       case when employee_id in (
            select employee_id from employee_breaks_view where shift_date =  cast(punch_apply_date as date)
           ) then true
        else false end as has_taken_break,
       (select break_hour  from employee_breaks_view b
       where b.employee_id = t.employee_id and b.shift_date = cast(t.punch_apply_date as date)) as break_hour,
        case when employee_id in (
                    select employee_id from employee_charge_view where shift_date =  cast(punch_apply_date as date)
                   ) then true
        else false end as was_charge,
        (select charge_hour  from employee_charge_view b
       where b.employee_id = t.employee_id and b.shift_date = cast(t.punch_apply_date as date)) as charge_hour,
       case when employee_id in (
                    select employee_id from employee_on_call_view where shift_date =  cast(punch_apply_date as date)
                   ) then true
        else false end as was_on_call,
        (select on_call_hour  from employee_on_call_view b
       where b.employee_id = t.employee_id and b.shift_date = cast(t.punch_apply_date as date)) as on_call_hour,
         (select absent_team_members  from absent_employee_department b
               where b.department_id = t.cost_center and b.shift_date = cast(t.punch_apply_date as date)) as num_teammates_absent
from raw_timesheet t group by employee_id, cost_center, punch_apply_date, paycode 
having paycode = 'WRK' or paycode='ABSENT' and employee_id <> 'employee_id' order by shift_date asc ;