** Creating an archive table or copy table of tables

An archive table or copy table is created of the tables which contains similar data as of the table. It can be used as a backup table so that we can retrieve the data if the main table is changed or transformed.


**1. Coonecting to the database**

[//]:comment

~~~ python
 def connect():
        return psycopg2.connect(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("database")
        )
~~~

Connection to the database using psycopg2. This method placed in utils.py and imported wherever it is needed in the script.

~~~ python
from utils import connect
~~~

---
~~~ python
 create table copy_raw_timesheet(
    employee_id	VARCHAR(500),
    cost_center	VARCHAR(500),
    punch_in_time VARCHAR(500),
    punch_out_time VARCHAR(500),
    punch_apply_date VARCHAR(500),
    hours_worked VARCHAR(500),
    paycode VARCHAR(500)
);

~~~

Creating copy_raw_timesheet and copy_raw_employee table to which we copy the tables afterwards.

---
**2. Truncating Previous Data from the Database**

~~~python

    def truncate_table(table_name):
        with open("../sql/queries/truncate_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))% table_name
            print(sql)
            source_cursor.execute(sql)       
            source_connection.commit()

~~~
The truncate_table.sql [a link](https://github.com/nischalbadal/data-warehouse-assignments-leapfrog/blob/day-3/assignment-day-3/src/sql/queries/truncate_table.sql) contains the truncate query. We pass the table_name parameter which is the name of table that will be truncated.

**3. Extracting the data from csv file into the database**
---
~~~ python
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
                        source_cursor.execute(insert_query, row)       
                        source_connection.commit()
~~~

The data is loaded from the CSV file and the first row is removed as it contains the attribute names of the data. We use the sql query to insert into the raw_timesheet table.

After that, we apply the operations into database using execute() method and commit the connection to the database.



**4. Extracting the XML file into the database**
---
~~~ python
     def copy_timesheet_data():
        sql = '''SELECT employee_id, first_name, last_name, department_id, 
                        department_name, manager_employee_id, employee_role, salary, hire_date, 
                        terminated_date, terminated_reason, dob ,fte ,location
                FROM raw_employee;
                '''
        source_cursor.execute(sql)
        result = source_cursor.fetchall()

        insert_query = '''INSERT INTO copy_raw_employee(employee_id, first_name, last_name, department_id, 
                        department_name, manager_employee_id, employee_role, salary, hire_date, 
                        terminated_date, terminated_reason, dob ,fte ,location) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        for row in result:
            row= tuple(row)
            dest_cursor.execute(insert_query, row)
            dest_connection.commit()   
~~~

Here, we select all the attributes from raw_timesheet table and insert to the new copy_raw_timesheet tabe using two connections and their cursors respectively. The

**5. Calling the Functions**
---
~~~ python
    if __name__ == '__main__':
        truncate_table("raw_timesheet")
        extract_timesheet_data("../../data/timesheet_2021_05_23.csv")
        extract_timesheet_data("../../data/timesheet_2021_06_23.csv")
        extract_timesheet_data("../../data/timesheet_2021_07_24.csv")
        copy_timesheet_data() 
~~~

All the above declared methods are called fom the main method.