# ETL Design- Day 5
## Loading of Timesheet Data 

After successful extraction and transformation, we now load the Timesheet data into the Data Warehouse previously designed. 


The steps for loading the data into the respective facts and dimension tables are as follows:


## 1. Extracting raw_timesheet table
Firstly, We add a function in _utils.py_ script which truncated the table and can be called elsewhere when needed.


```python
 def truncate_table(table_name, con, cur):
    with open("../sql/queries/truncate_table.sql") as f:
        sql = ' '.join(map(str, f.readlines()))% table_name
        print(sql)
        cur.execute(sql)       
        con.commit()
```
The _truncate_table.sql_ file contains are: 
```sql
TRUNCATE TABLE %s RESTART IDENTITY CASCADE;
```
The RESTART IDENTITY restarts the surrogate key which is mostly the primary key of the tables. This is done to avoid mismatch of id columns while loading the data.

## 2. Extracting and Archiving the raw_timesheet table

The structure of main _timesheet_ table was given as below:

```python
  def extract_timesheet_data(fileName, con, cur):
        with open(fileName, 'r') as f:
            i = 0
            for line in f:
                if i==0:
                    i+=1
                    continue
                row = line[:-1].split(",")
                with open("../sql/queries/extract_raw_timesheet_data.sql") as f:
                    insert_query = ' '.join(map(str, f.readlines())) 
                    cur.execute(insert_query, row)     
                    con.commit()
        print("Extraction successful to raw_timesheet table.") 
```
This creates a _raw_timesheet_ table which contains all the raw data of the timesheet stored in the database.

```python
  def archive_timesheet_data(con, cur):
        with open("../sql/queries/copy_raw_timesheet.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()
        print("Archiving successful to copy_raw_timesheet table.") 
```
This function creates the archive copy or _raw_timesheet_ table which is used as backup for the further process of transformation and load of timesheet data.

## 3. Transforming  timesheet table
```python
  def transform_timesheet_data(con, cur):
       # Creating an intermediate table employee_shift_timings
        with open("../../schema/create_employee_shift_timings_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Creating an intermediate table: ")
            print(sql)
            cur.execute(sql)       
            con.commit()

        # Loading the intermediate table 
        truncate_table("employee_shift_timings", con, cur)
        with open("../sql/queries/load_employee_shift_timings_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Transforming the Intermediate Table: ")
            print(sql)
            cur.execute(sql)       
            con.commit()

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
            cur.execute(sql)       
            con.commit() 
        
        # transforming and loading the timesheet table
        truncate_table("timesheet", con, cur)
        with open("../sql/queries/load_transformed_timesheet_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print("Creating Views: ")
            print(sql)
            cur.execute(sql)       
            con.commit() 
```
We initially truncate the table to clear all the datas and restart the surrogate keys so that the transformation process and loading process is successfully completed.
The detailed documentation for transformation of timesheet table is [here](https://github.com/nischalbadal/data-warehouse-assignments-leapfrog/blob/day-4/assignment-day-4/docs/ETL%20Design-%20Day%204.md)

After this pricess, transformed table named _timesheet_ is loaded with the data from the _raw_timesheet_ table.
## 4. Loading the dimensions table 

After all the dimension tables are created, we now load the tables and finally load the fact table referencing to the dimensions table using referential integrity constraints.

The methods for loading the dimension tables related with the _fact_timesheet_ table are as below:

```python
  def load_dim_shift_type(con, cur):
        truncate_table("dim_shift_type", con, cur)
        with open("../sql/queries/extract_dim_shift_type.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_shift_type Success !')
```
Here, _dim_shift_ table is loaded with the unique _shift_type_ from the transformed timesheet table. The similar process is done with all other dimensions table linked to _fact_timesheet_ table.
```python
 def load_dim_period(con, cur):
        truncate_table("dim_period", con, cur)
        with open("../sql/queries/extract_dim_period.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_period Success !')
```

## 4. Loading the Fact table

After all the dimension tables are loaded, now we finally load the fact table relating to the dimensions table.

The DDL syntax to create _fact_timesheet_ table is given below.

```sql
CREATE TABLE fact_timesheet(
    employee_id  INT NOT NULL,
    work_date DATE NOT NULL,
    department_id INT NOT NULL,
    hours_worked FLOAT NOT NULL,
    shift_type_id VARCHAR(255) NOT NULL,
    punch_in_time TIMESTAMP NOT NULL,
    punch_out_time TIMESTAMP NOT NULL,
    time_period_id INT NOT NULL,
    attendance VARCHAR(100) NOT NULL,
    has_taken_break BOOLEAN NOT NULL,
    break_hour FLOAT NOT NULL,
    was_charge BOOLEAN NOT NULL,
    charge_hour FLOAT NOT NULL,
    was_on_call BOOLEAN NOT NULL,
    on_call_hour FLOAT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    num_teammates_absent INT NOT NULL,
    CONSTRAINT fk_department_id FOREIGN KEY (department_id) REFERENCES dim_department(id),
    CONSTRAINT fk_employee_id FOREIGN KEY (employee_id) REFERENCES fact_employee (employee_id)
);


```

Now we select all the attributes from the transformed table to load into the _fact_timesheet_ table:

```python
 def load_fact_timesheet(con,cur):
        truncate_table("fact_timesheet", con, cur)
        with open("../sql/queries/extract_fact_timesheet.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading fact_timesheet Success !')
```
The SQL query to load the _fact_timesheet_ table is shown below:

```sql
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
```
After this query successfully executes, we have loaded the following table following ETL process:
_dim_shift_type_, _dim_period_, and _fact_timesheet_.

## 5. Implementing in python

The main() function of our python script file to perform all above mentioned functionalities is below:
```python
 def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_timesheet", con, cur)
        truncate_table("copy_raw_timesheet", con, cur)

        extract_timesheet_data("../../data/timesheet_2021_05_23.csv",con,cur)
        extract_timesheet_data("../../data/timesheet_2021_06_23.csv",con,cur)
        extract_timesheet_data("../../data/timesheet_2021_07_24.csv",con,cur)
        archive_timesheet_data(con,cur)

        truncate_table("fact_timesheet", con, cur)
        load_dim_shift_type(con, cur)
        load_dim_period(con, cur)
        load_fact_timesheet(con, cur)
    
        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

```
In this way we extract, transform and load the Timesheet data into a _timesheet_ table. 