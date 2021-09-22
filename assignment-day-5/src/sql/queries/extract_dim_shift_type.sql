insert into dim_shift_type(name)
select
distinct shift_type
from timesheet where shift_type <> '';

