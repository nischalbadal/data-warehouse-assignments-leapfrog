CREATE VIEW employee_charge_view
as
  select employee_id, cast(punch_apply_date as date) as shift_date , sum(cast(hours_worked as float)) as charge_hour from raw_timesheet
            group by employee_id,  punch_apply_date, paycode
            having paycode = 'CHARGE';
select * from employee_charge_view;

CREATE VIEW employee_breaks_view
as
  select employee_id, cast(punch_apply_date as date) as shift_date , sum(cast(hours_worked as float)) as break_hour from raw_timesheet
            group by employee_id,  punch_apply_date, paycode
            having paycode = 'BREAK';
select * from employee_breaks_view;

CREATE VIEW employee_on_call_view
as
  select employee_id, cast(punch_apply_date as date) as shift_date , sum(cast(hours_worked as float)) as on_call_hour from raw_timesheet
            group by employee_id,  punch_apply_date, paycode
            having paycode = 'ON_CALL';
select * from employee_on_call_view;


CREATE VIEW absent_employee_department
as
select  cost_center as department_id, cast(punch_apply_date as date) as shift_date, count(*) as absent_team_members
from raw_timesheet group by  cost_center, punch_apply_date, paycode
having paycode='ABSENT';

