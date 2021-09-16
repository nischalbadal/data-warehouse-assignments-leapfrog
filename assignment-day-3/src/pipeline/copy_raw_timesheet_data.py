from utils import connect

try:
    source_connection = connect()
    dest_connection = connect()
    source_cursor = source_connection.cursor()
    dest_cursor = dest_connection.cursor()

    def truncate_table(table_name):
        with open("../sql/queries/truncate_table.sql") as f:
            sql = ' '.join(map(str, f.readlines()))% table_name
            print(sql)
            source_cursor.execute(sql)       
            source_connection.commit()
        

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
                     

    if __name__ == '__main__':
        truncate_table("raw_timesheet")
        extract_timesheet_data("../../data/timesheet_2021_05_23.csv")
        extract_timesheet_data("../../data/timesheet_2021_06_23.csv")
        extract_timesheet_data("../../data/timesheet_2021_07_24.csv")
        copy_timesheet_data()

except Exception as e:
     print('Error: ' + str(e))

finally:
    source_connection.close()
    dest_connection.close()