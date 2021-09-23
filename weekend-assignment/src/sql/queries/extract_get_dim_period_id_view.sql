CREATE VIEW get_dim_period_id_view AS(
    SELECT DISTINCT
          CASE WHEN s.bill_date = '2017-02-30 11:00:00'
          THEN TO_DATE('2017-02-28', 'YYYY-MM-DD') ELSE TO_DATE(s.bill_date, 'YYYY-MM-DD') END as bill_date,
           (SELECT id from dim_period p
            WHERE p.start_date <= (CASE WHEN s.bill_date = '2017-02-30 11:00:00'
                THEN TO_DATE('2017-02-28', 'YYYY-MM-DD') ELSE TO_DATE(s.bill_date, 'YYYY-MM-DD') END)
               AND p.end_date > (CASE WHEN s.bill_date = '2017-02-30 11:00:00'
                THEN TO_DATE('2017-02-28', 'YYYY-MM-DD') ELSE TO_DATE(s.bill_date, 'YYYY-MM-DD') END)
               )
               as time_period_id
    from raw_sales s
);