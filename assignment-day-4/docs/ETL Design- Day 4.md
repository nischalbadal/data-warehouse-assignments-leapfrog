# ETL Design- Day 4
## Transformation of HR Timesheet Data 

We breakdown the steps of transformation and loading of timesheet data from raw_timesheet table into different steps as mentioned below:

- Making the archive of data of raw_timesheet table
- Creating main _timesheet_ table 
- Creating an intermediate table named _employee_shift_timings_ which groups the timesheet records by _employee_id_, _punch_apply_date_ and _paycode_ as it only stores WRK paycode.
- We now load the data into the _employee_shift_timings_ table by applying query from the _raw_timesheet_ table.
- We create view to calculate _has_taken_break_ flag and _break_hour_ as sum of all breaks taken by a employee in one shift day.
- Similarly, views are created for _was_charge_ - _charge_hour_, _was_on_call_ - _on_call_hour_ and _num_teammates_absent_ columns in the transformed table.
- We transform _raw_timesheet_ table by applying queries to obtain all the required attributes to match _timesheet_ table as _attendance_, _shift_start_time_, _shift_end_time_, etc.
- Finally, the data is loaded into _timesheet_ table. In this way transformation is done.

## Description of above mentioned steps: 



## 1. Copying the _raw_timesheet_ table into _copy_raw_timesheet_ table


```python
 def copy_timesheet_data():
        with open("../sql/queries/copy_raw_timesheet.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cursor.execute(sql)
            connection.commit()

        print("Archiving successful to copy_raw_timesheet table.") 
```
The _copy_raw_timesheet.sql_ file contains are: 
```sql
insert into copy_raw_timesheet(employee_id, cost_center, punch_in_time,
punch_out_time, punch_apply_date, hours_worked, paycode)
select * from raw_timesheet;
```
This makes an archive copy of _raw_timesheet_ table which can be used as a backup while transforming _raw_timesheet_ table into _timesheet_ table

## 2. Creating main _timesheet_ table

The structure of main _timesheet_ table was given as below:

| timesheet |
| ------ |
| employee_id | 
| department_id |
| shift_start_time |
| shift_end_time | 
| shift_date | 
| shift_type |
| hours_worked |
| attendance |
| has_taken_break |
| was_charge |
| charge_hour |
| was_on_call |
| on_call_hour |
| num_teammates_absent |

The DDL syntax for creating the table is:

```sql
create table if not exists timesheet(
    employee_id VARCHAR(250),
    department_id VARCHAR(250),
    shift_start_time TIME,
    shift_end_time TIME,
    shift_date DATE,
    shift_type VARCHAR(100),
    hours_worked FLOAT,
    attendance VARCHAR(100),
    has_taken_break BOOLEAN,
    break_hour FLOAT,
    was_charge BOOLEAN,
    charge_hour FLOAT,
    was_on_call BOOLEAN,
    on_call_hour FLOAT,
    num_teammates_absent INT
);
```

## 3. Creating an intermediate table 

We create an intermediate table named employee_shift_timings which groups the timesheet records by employee_id, punch_apply_date and paycode as it only stores 'WRK' paycode.

This table sorts the employee records as per date so that we get a single record of employee in a single date. 

To find _shift_start_time_, we take the minimum of the _punch_in_time_ of that day without including the null records in the _punch_in_time_. SImilarly to find _shift_end_time_, we take the maximum of the _punch_out_time_ of that specific _punch_apply_date_ which is now referred as _shift_date_.

###### Note : while extracting the raw tables, we have defined the data type as varchar for every attribute to avoid date format mismatch. Now while transforming, we define their suitable datatype and cast the varchar data from the raw table.  
--
The _shift_type_ is calculated by applying case when --- then --- end condition of postgresql. If the _shift_start_time_ starts 5:00 in the morning to 12:00 then it refers to Morning. Similarly, after 12:00 in the noon to 5:00 it refers as Afternoon. The time format in our raw data is in 24 hours format. Hence, we cast the _punch_in_time_ as time with 24 hours conversion format.

The loading query into employee_shift_timings table is below:

```sql
INSERT INTO employee_shift_timings(employee_id, shift_date, shift_start_time, shift_end_time, shift_type, hours_worked)
select employee_id,
    cast(punch_apply_date as date) as shift_date,
    min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) as shift_start_time,
    max(to_timestamp (punch_out_time, 'YYYY.MM.DD HH24:MI:SS')::time) as shift_end_time,
    (case when min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '05:00:00.000000' and
     min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '12:00:00' then 'Morning'
     when min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '12:00:00.000000' and
     min(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '17:00:00' then 'Afternoon'    end) as shift_type,
     sum(cast(hours_worked as float))
from raw_timesheet r group by employee_id, punch_apply_date, paycode
having paycode= 'WRK' order by shift_date desc;
```
Here, we group by only for the 'WRK' paycodes which means that an employee is working.

## 4. Creating views for other calculations

For other columns in the _timesheet_ table, we create virtual tables i.e. views so that we can filter them by _employee_id_ and _shift_date_ and display the value in our main table.

Some of the views that are created are:

```sql
CREATE VIEW employee_charge_view
as
  select employee_id, cast(punch_apply_date as date) as shift_date , sum(cast(hours_worked as float)) as charge_hour from raw_timesheet
            group by employee_id,  punch_apply_date, paycode
            having paycode = 'CHARGE';
select * from employee_charge_view;
```
This view displays which employee was on charge on a specific date and the total hours worked as a charge in that _shift_date_.

```sql
CREATE VIEW employee_breaks_view
as
  select employee_id, cast(punch_apply_date as date) as shift_date , sum(cast(hours_worked as float)) as break_hour from raw_timesheet
            group by employee_id,  punch_apply_date, paycode
            having paycode = 'BREAK';
select * from employee_breaks_view;
```
The _employee_breaks_view_ generated a table of employee break records on a specific _shift_date_ and also total break hours on an employee on a single _shift_date_.

```sql
CREATE VIEW employee_on_call_view
as
  select employee_id, cast(punch_apply_date as date) as shift_date , sum(cast(hours_worked as float)) as on_call_hour from raw_timesheet
            group by employee_id,  punch_apply_date, paycode
            having paycode = 'ON_CALL';
select * from employee_on_call_view;
```
The _employee_on_call_view_ generated a table of employee ON_CALL paycode records on a specific _shift_date_ and also total on_call hours on an employee on a single _shift_date_.

```sql
CREATE VIEW absent_employee_department
as
select  cost_center as department_id, cast(punch_apply_date as date) as shift_date, count(*) as absent_team_members
from raw_timesheet group by  cost_center, punch_apply_date, paycode
having paycode='ABSENT';
```
And the final view _absent_employee_department_ groups total number of absent employee per department in a specific _shift_date_.

## Transforming the columns for _timesheet_ table

After all the intermediate tables and views are successfully created, we now transform the _raw_timesheet_ table to obtain attributes for the _timesheet_ table. This is one by using many subqueries as selecting from intermediate tables and selecting from views by filtering with _employee_id_ and _shift_date_ and _department_id_ in some case. 

The final sql query that transforms the _raw_timesheet_ table attributes to match _timesheet_ table is:
```sql
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
```
The above select query is grouped by _employee_id_, _cost_center_ or also called _department_id_, _punch_apply_date_ i.e. _shift_date_, and _paycode_ filtering _paycode_ as 'WRK' and 'ABSENT' only.

There is a condition which checks ``` employee_id <> 'employee_id' ```. This is because if the data extraction is done using ```\COPY ``` command, the header is present as the row in the _raw_timesheet_ table. This condition filters out that record. 

We now finally load the transformed data into the _timesheet_table. This can be done by either creating another table to store the above selected result or by looping through each columns in python. As they are in the same database server, one efficient way is to direct insert into _timesheet_table. 

That can be done by the following query: 
```sql
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
```


## Implementing the solution in python

In python, we load the queries and run them through the database connection. Since the queries are big, they are placed in a file and loaded from there using file operations : readlines().

The python function for loading the transformed data is:

```python
def load_transformed_timesheet_table():
        # Creating an intermediate table employee_shift_timings
        with open("../../schema/create_employee_shift_timings_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Creating an intermediate table: ")
            print(sql)
            cursor.execute(sql)       
            connection.commit()

        # Loading the intermediate table 
        with open("../sql/queries/load_employee_shift_timings_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Transforming the Intermediate Table: ")
            print(sql)
            cursor.execute(sql)       
            connection.commit()

        #dropping views if previously exists
        drop_view("employee_charge_view")
        drop_view("employee_breaks_view")
        drop_view("employee_on_call_view")
        drop_view("absent_employee_department")

        # creating views for transformation
        with open("../../schema/create_views_for_transformation.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Creating Views: ")
            print(sql)
            cursor.execute(sql)       
            connection.commit() 
        
        # transforming and loading the timesheet table
        with open("../sql/queries/load_transformed_timesheet_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Creating Views: ")
            print(sql)
            cursor.execute(sql)       
            connection.commit() 


    def truncate_table(table_name):
        with open("../sql/queries/truncate_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))% table_name
            print(sql)
            cursor.execute(sql)       
            connection.commit()
    
    def drop_view(view_name):
        with open("../sql/queries/drop_view.sql") as f:
            sql = ' '.join(map(str, f.readlines()))% view_name
            print(sql)
            cursor.execute(sql)       
            connection.commit()

    def extract_timesheet_data(filename):
        with open(filename, 'r') as f:
            i = 0
            for line in f:
                if i==0:
                    i+=1
                    continue
                row = line[:-1].split(",")
                with open("../sql/queries/extract_raw_timesheet_data.sql") as f:
                    insert_query = ' '.join(map(str, f.readlines())) 
                    cursor.execute(insert_query, row)       
                connection.commit()

        print("Extraction successful to raw_timesheet table.")    
```

Here, all the above mentioned queries are stored in the respective files as ```schemas/``` store DDL queries and ```src/sql/queries/``` store other queries..

In this way we transformed HR Timesheet data into a _timesheet_ table. 