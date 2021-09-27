from utils import *

try:
    def load_dim_period(con, cur):
        truncate_table("dim_period", con, cur)
        with open("../sql/queries/extract_dim_period_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()
        print("Loading dim_period table successful.") 

    def load_related_views(con, cur):
        with open("../sql/queries/extract_get_dim_period_id_view.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()
        print("Loading dim_period_id_view view successful.") 

    def load_fact_sale_product(con, cur):
        with open("../sql/queries/extract_fact_sale_product_data.sql") as f:
            sql = ' '.join(map(str, f.readlines())) 
            cur.execute(sql)
            con.commit()
        print("Transformation and Loading successful to fact_sale_product table.") 


    def main():
        con = connect()
        cur = con.cursor()

        truncate_table("fact_sale_product", con, cur)
        
        load_dim_period(con, cur)
        load_fact_sale_product(con, cur)

        cur.close()
        con.close()

    if __name__ == '__main__':
        main()

except Exception as e:
     print('Error: ' + str(e))

    

