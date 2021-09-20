from utils import *

try:
   
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
    
       
    def archive_timesheet_data(con, cur):
        with open("../sql/queries/copy_raw_timesheet.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()
        print("Archiving successful to copy_raw_timesheet table.") 


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
    
  
    def load_dim_shift_type(con, cur):
        truncate_table("dim_shift_type", con, cur)
        with open("../sql/queries/extract_dim_shift_type.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_shift_type Success !')
        

    def load_dim_period(con, cur):
        truncate_table("dim_period", con, cur)
        with open("../sql/queries/extract_dim_period.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_period Success !')
        

    def load_fact_timesheet(con,cur):
        truncate_table("fact_timesheet", con, cur)
        with open("../sql/queries/extract_fact_timesheet.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading fact_timesheet Success !')

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

except Exception as e:
     print('Error: ' + str(e))

    

