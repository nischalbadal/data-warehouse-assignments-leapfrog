from utils import *

try:
   
    def extract_product_data(fileName, con, cur):
        with open(fileName, 'r') as f:
            i = 0
            for line in f:
                if i==0:
                    i+=1
                    continue
                row = line[:-1].split(",")
                with open("../sql/queries/extract_raw_product_data.sql") as f:
                    insert_query = ' '.join(map(str, f.readlines())) 
                    cur.execute(insert_query, row)     
                    con.commit()

        print("Extraction successful to raw_product table.") 
    
       
    def archive_product_data(con, cur):
        with open("../sql/queries/extract_copy_raw_product_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()

        print("Archiving successful to copy_raw_product table.") 

    def load_dim_brand(con, cur):
        truncate_table("dim_brand", con, cur)
        with open("../sql/queries/extract_dim_brand_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()

        print("Loading of dim_brand table successful.") 
    
    def load_dim_category(con, cur):
        truncate_table("dim_category", con, cur)
        with open("../sql/queries/extract_dim_category_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()

        print("Loading of dim_category table successful.") 
    
    def load_dim_active_status(con, cur):
        truncate_table("dim_active_status", con, cur)
        with open("../sql/queries/extract_dim_active_status_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()

        print("Loading of dim_active_status table successful.") 


    def load_fact_product(con, cur):
        with open("../sql/queries/extract_fact_product_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()

        print("Loading of fact_product table successful.") 


    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("raw_product", con, cur)
        truncate_table("copy_raw_product", con, cur)

        extract_product_data("../../data/product_dump.csv",con,cur)
        archive_product_data(con, cur)

        truncate_table("fact_product", con, cur)
        load_dim_brand(con, cur)
        load_dim_category(con, cur)
        load_dim_active_status(con, cur)
        load_fact_product(con, cur)

        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

