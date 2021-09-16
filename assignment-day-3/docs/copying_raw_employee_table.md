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

create table copy_raw_employee(
    employee_id VARCHAR(500),
    first_name VARCHAR(500),
    last_name VARCHAR(500),
    department_id VARCHAR(500),
    department_name VARCHAR(500),
    manager_employee_id VARCHAR(500),
    employee_role VARCHAR(500),
    salary VARCHAR(500),
    hire_date VARCHAR(500),
    terminated_date VARCHAR(500),
    terminated_reason VARCHAR(500),
    dob VARCHAR(500),
    fte VARCHAR(500),
    location VARCHAR(500)
);
~~~

Creating  copy_raw_employee table to which we copy the tables afterwards.

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

**3. Extracting the data from json file into the database**
---
~~~ python
      def extract_employee_data_json(fileName):
            with open(fileName, 'r') as f:
                record_list = json.load(f)

                values = [list(x.values()) for x in record_list]
                columns = [list(x.keys()) for x in record_list][0]      
                values_str = ""
                for i, record in enumerate(values):
                    val_list = []
                    for v, val in enumerate(record):
                        if type(val) == str:
                            val = str(Json(val)).replace('"', '')
                        val_list += [ str(val) ]
                    values_str += "(" + ', '.join( val_list ) + "),\n"
            
                values_str = values_str[:-2] + ";"   
                insert_query = '''
                            INSERT INTO raw_employee (%s) 
                            VALUES %s
                            ''' %(','.join(columns),values_str)
                source_cursor.execute(insert_query)       
                source_connection.commit()
~~~

The json file is loaded using the json.load() method of json module. we now separate the values and columns and format the columns to format the INSERT INTO table_name VALUES() statement. 

After that, we insert the values using execute() method and commit the connection to the database.

**4. Extracting the XML file into the database**
---
~~~ python
     def extract_employee_data_xml(fileName):
        root = etree.parse(fileName)
        for i in root.findall("Employee"):
            values = [i.find(n).text for n in ("employee_id", "first_name", "last_name", "department_id", "department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location")]
            values = ['' if v is None else v for v in values]
            value_str = "(" + str(values)[1:-1] + ");"
            insert_query = '''
                            INSERT INTO raw_employee ("employee_id", "first_name", "last_name", "department_id", "department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location") 
                            VALUES %s
                            ''' %(value_str)
          
            source_cursor.execute(insert_query)       
            source_connection.commit()
~~~

We use etree module to extract the data from the XML file and save it to the database. We check for all root "Employee" tags and under that for the attributes that are to be inserted. 

There can be None values in the table which will cause the insert to fail so replace the None by ''. Now we insert the data into the raw_employee table. 

**5. Extracting the XML file into the database**
---
~~~ python
     def copy_employee_data():
        sql = '''SELECT employee_id,first_name,last_name,department_id,department_name,manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,fte,location
                FROM raw_employee;
                '''
        source_cursor.execute(sql)
        result = source_cursor.fetchall()
     
        insert_query = '''INSERT INTO copy_raw_employee(employee_id,first_name,last_name,department_id,department_name,
                        manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,
                        fte,location)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        for row in result:
            row= tuple(row)
            dest_cursor.execute(insert_query, row)
            dest_connection.commit()   
~~~

Here, we select all the attributes from raw_employee table and insert to the new copy_raw_employee tabe using two connections and their cursors respectively.

**6. Calling the Functions**
---
~~~ python
    if __name__ == '__main__':
        truncate_table("raw_employee")
        extract_employee_data_json("../../data/employee_2021_08_01.json")
        extract_employee_data_xml("../../data/employee_2021_08_01.xml")
        copy_employee_data()
~~~

All the above declared methods are called fom the main method.
