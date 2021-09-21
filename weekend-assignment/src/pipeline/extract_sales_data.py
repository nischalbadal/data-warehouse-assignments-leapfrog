from utils import *

try:
   
    def extract_sales_data(fileName, con, cur):
        with open(fileName, 'r') as f:
            i = 0
            for line in f:
                if i==0:
                    i+=1
                    continue
                row = line[:-1].split(",")
                with open("../sql/queries/extract_raw_sales_data.sql") as f:
                    insert_query = ' '.join(map(str, f.readlines())) 
                    cur.execute(insert_query, row)     
                    con.commit()
        print("Extraction successful to raw_sales table.") 
    
       
    def archive_sales_data(con, cur):
        with open("../sql/queries/copy_raw_sales.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()
        print("Archiving successful to copy_raw_sales table.") 


    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_sales", con, cur)
        truncate_table("copy_raw_sales", con, cur)

        extract_sales_data("../../data/sales_dump.csv",con,cur)
     

        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

