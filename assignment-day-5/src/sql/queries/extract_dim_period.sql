insert into dim_period(start_date, end_date)
select
       min(shift_date) as start_date,
       max(shift_date) as end_date
from timesheet;

