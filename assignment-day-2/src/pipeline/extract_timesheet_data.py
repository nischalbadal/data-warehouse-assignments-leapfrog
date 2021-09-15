import psycopg2

def connect():
    return psycopg2.connect(
        host = 'localhost',
        database = 'data-internship',
        user='postgres',
        password='nischal541',
        port = 5432
    )

try:
    connection = connect()
    cursor = connection.cursor()

    def truncate_timesheet_data():
            with open("../sql/queries/truncate_timesheet_data.sql") as f:
                sql = ' '.join(map(str, f.readlines()))
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
                    with open("../sql/queries/extract_timesheet_data.sql") as f:
                        insert_query = ' '.join(map(str, f.readlines()))
                       
                        cursor.execute(insert_query, row)       
                        connection.commit()
                        

    if __name__ == '__main__':
        truncate_timesheet_data()
        extract_timesheet_data("../../data/timesheet_2021_05_23.csv")
        extract_timesheet_data("../../data/timesheet_2021_06_23.csv")
        extract_timesheet_data("../../data/timesheet_2021_07_24.csv")

except Exception as e:
     print('Error: ' + str(e))

finally:
     cursor.close()
     connection.close()