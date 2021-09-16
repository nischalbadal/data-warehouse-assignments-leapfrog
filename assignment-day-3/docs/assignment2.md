ETL Design - Day 3 Assignment

Prepared by Nischal badal


**1. Extract data from a  Database**

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

connect to the database using psycopg2

---
~~~ python
  CREATE TABLE raw_sales_db(
    user_id VARCHAR(255),
    username VARCHAR(255),
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    category_id VARCHAR(255),
    category_name VARCHAR(255),
    current_price VARCHAR(255),
    sold_price VARCHAR(255),
    sold_quantity VARCHAR(255),
    remaining_quantity VARCHAR(255),
    sales_date VARCHAR(255)
);
~~~

Creating Raw Sales table to extract the data into.


---
**2. Truncating Previous Data from the Database**

~~~python
 def truncate_existing_data():
        table_name ="raw_sales_db"
        with open("../sql/queries/truncate_table.sql") as f:
            sql = ' '.join(map(str, f.readlines())) % table_name
            print(sql)
            source_cursor.execute(sql)       
            source_connection.commit()

~~~

**2. Extracting the joined data into the database**
---
~~~ python
      def extract_sales_db_data():
    
        with open("../sql/queries/extract_raw_sales_data.sql") as f:
            sql = "".join(f.readlines())
            source_cursor.execute(sql)
            res = source_cursor.fetchall()

            sql = 'INSERT INTO raw_sales_db(user_id, username, product_id, product_name, category_id,category_name,' \
                  'current_price,sold_price,sold_quantity,remaining_quantity,sales_date)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            for row in res:
                dest_cursor.execute(sql,row)
                dest_connection.commit()
~~~

