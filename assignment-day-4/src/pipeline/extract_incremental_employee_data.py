from utils import connect

try:
    connection = connect()
    cursor = connection.cursor()

    def truncate_existing_data():
        table_name ="raw_sales_db"
        with open("../sql/queries/truncate_table.sql") as f:
            sql = ' '.join(map(str, f.readlines())) % table_name
            print(sql)
            cursor.execute(sql)       
            connection.commit()

    def extract_employee_data(fileName):   

        with open(fileName, 'r') as f:
            i = 0
            for line in f:
                if i==0:
                    i+=1
                    continue
                row = line[:-1].split(",")
                with open("../sql/queries/extract_employee_data.sql") as f:
                    insert_query = ' '.join(map(str, f.readlines())) 
                    cursor.execute(insert_query, row)     
                    connection.commit()

        print("Extraction successful to Employee table.") 
    
    def transform_employee_data():
          with open("../sql/queries/load_employee_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print(sql)
            cursor.execute(sql)       
            connection.commit()
        
    if __name__ == "__main__":
        truncate_existing_data()
        extract_employee_data("../../data/employee_2021_08_01.csv")
        transform_employee_data()
        # extract_employee_data("../../data/employee_incremental_2021_08_02.csv")
        # extract_employee_data("../../data/employee_incremental_2021_08_03.csv")

except Exception as e:
    print(e)

finally:
    cursor.close()
    connection.close()

    