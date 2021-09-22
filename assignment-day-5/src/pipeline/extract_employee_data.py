from utils import *

try:
   
    def extract_employee_data(fileName, con, cur):
        with open(fileName, 'r') as f:
            i = 0
            for line in f:
                if i==0:
                    i+=1
                    continue
                row = line[:-1].split(",")
                with open("../sql/queries/extract_employee_data.sql") as f:
                    insert_query = ' '.join(map(str, f.readlines())) 
                    cur.execute(insert_query, row)     
                    con.commit()
        print("Extraction successful to raw_employee table.") 
    
       
    def archive_employee_data(con, cur):
        sql = '''SELECT employee_id,first_name,last_name,department_id,department_name,manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,fte,location
                FROM raw_employee;
                '''
        cur.execute(sql)
        result =cur.fetchall()
     
        insert_query = '''INSERT INTO copy_raw_employee(employee_id,first_name,last_name,department_id,department_name,
                        manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,
                        fte,location)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        for row in result:
            row= tuple(row)
            cur.execute(insert_query, row)
            con.commit() 

    def transform_employee_data(con, cur):
        truncate_table("employee", con, cur)
        with open("../sql/queries/load_employee_table.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
    
  
    def load_dim_role(con, cur):
        truncate_table("dim_role", con, cur)
        with open("../sql/queries/extract_dim_role.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_role Success !')
        

    def load_dim_manager(con, cur):
        truncate_table("dim_manager", con, cur)
        with open("../sql/queries/extract_dim_manager.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_manager Success !')
        

    def load_dim_department(con, cur):
        truncate_table("dim_department", con, cur)
        with open("../sql/queries/extract_dim_department.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_department Success !')
        
    
    def load_dim_status(con, cur):
        truncate_table("dim_status", con, cur)
        with open("../sql/queries/extract_dim_status.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading dim_status Success !')

    def load_fact_employee(con,cur):
        truncate_table("fact_employee", con, cur)
        with open("../sql/queries/load_fact_employee.sql") as f:
          sql = ' '.join(map(str, f.readlines()))
          print(sql)
          cur.execute(sql)       
          con.commit()
        print('Loading fact_employee Success !')

    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_employee", con, cur)
        truncate_table("copy_raw_employee", con, cur)

        extract_employee_data("../../data/employee_2021_08_01.csv",con,cur)
        archive_employee_data(con,cur)

        truncate_table("fact_employee", con, cur)
        load_dim_role(con, cur)
        load_dim_manager(con, cur)
        load_dim_department(con, cur)
        load_dim_status(con, cur)
        load_fact_employee(con, cur)
    

        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

