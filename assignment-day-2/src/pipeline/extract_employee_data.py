from utils import connect
import json
from psycopg2.extras import Json
from lxml import etree


try:
    connection = connect()
    cursor = connection.cursor()

    def truncate_employee_data():
            with open("../sql/queries/truncate_employee_data.sql") as f:
                sql = ' '.join(map(str, f.readlines()))
                print(sql)
                cursor.execute(sql)       
                connection.commit()
    

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
                cursor.execute(insert_query)       
                connection.commit()
    
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
          
            cursor.execute(insert_query)       
            connection.commit()


    if __name__ == '__main__':
        truncate_employee_data()
        extract_employee_data_json("../../data/employee_2021_08_01.json")
        extract_employee_data_xml("../../data/employee_2021_08_01.xml")

except Exception as e:
    print('Error: ' + str(e))
    
finally:
    cursor.close()
    connection.close()