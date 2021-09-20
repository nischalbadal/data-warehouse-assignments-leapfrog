INSERT INTO dim_status(name)
select distinct
case when term_date is not null then 'Terminated' else 'Active' end as term_status
from employee;

