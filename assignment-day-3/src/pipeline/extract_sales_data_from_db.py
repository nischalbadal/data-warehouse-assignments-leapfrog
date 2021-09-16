from utils import connect

try:
    source_connection = connect()
    dest_connection = connect()
    source_cursor = source_connection.cursor()
    dest_cursor = dest_connection.cursor()

    def truncate_existing_data():
        table_name ="raw_sales_db"
        with open("../sql/queries/truncate_table.sql") as f:
            sql = ' '.join(map(str, f.readlines())) % table_name
            print(sql)
            source_cursor.execute(sql)       
            source_connection.commit()

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


    if __name__ == "__main__":
        truncate_existing_data()
        extract_sales_db_data()

except Exception as e:
    print(e)

finally:
    source_connection.close()
    dest_connection.close()