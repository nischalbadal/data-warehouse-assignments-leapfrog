from utils import connect

try:
    connection = connect()
    cursor = connection.cursor()

    def create_timesheet_table():
        with open("../../schema/create_timesheet_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))
            print(sql)
            cursor.execute(sql)       
            connection.commit()

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
    
    def copy_timesheet_data():
        with open("../sql/queries/copy_raw_timesheet.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cursor.execute(sql)
            connection.commit()

        print("Archiving successful to copy_raw_timesheet table.")  
                     
    if __name__ == '__main__':
        truncate_table("raw_timesheet")
        truncate_table("copy_raw_timesheet")
        extract_timesheet_data("../../data/timesheet_2021_07_24.csv")
        copy_timesheet_data()
        create_timesheet_table()
        load_transformed_timesheet_table()



except Exception as e:
     print('Error: ' + str(e))

finally:
    cursor.close()
    connection.close()


