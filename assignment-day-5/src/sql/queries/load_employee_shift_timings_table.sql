INSERT INTO employee_shift_timings(employee_id, shift_date, shift_start_time, shift_end_time, shift_type, hours_worked)
select employee_id,
       cast(punch_apply_date as date) as shift_date,
       min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) as shift_start_time,
       max(to_timestamp (punch_out_time, 'YYYY.MM.DD HH24:MI:SS')::time) as shift_end_time,
       (case when min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '05:00:00.000000' and
                  min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '12:00:00' then 'Morning'
           when min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '12:00:00.000000' and
                  min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '17:00:00' then 'Afternoon'
          end) as shift_type,
       sum(cast(hours_worked as float))
from raw_timesheet r group by employee_id, punch_apply_date, paycode
having paycode= 'WRK' order by shift_date desc;

