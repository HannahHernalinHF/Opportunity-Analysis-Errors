
WITH VIEW_1 AS (SELECT DISTINCT fk_customer,
                                hellofresh_week,
                                CASE
                                    WHEN hellofresh_week < '2023-W27' THEN 'H1'
                                    WHEN hellofresh_week >= '2023-W27' THEN 'H2' END AS box_year_part,
                                box_id
                FROM materialized_views.box_production_dimensions
                WHERE hellofresh_week LIKE '2023%'
                  AND fk_customer <> '-1'
                  AND mapped_dc IN
                      ('Auckland', 'Banbury', 'Barleben', 'Bjuv', 'Dublin', 'Koelliken', 'Lisses', 'Madrid',
                       'Melbourne', 'Milan', 'Nuneaton', 'Oslo', 'Perth', 'Prismalaan', 'Sydney', 'Verden')
                )

   , VIEW_2 AS (SELECT DISTINCT complaint_item_id,
                                box_id,
                                CASE
                                    WHEN dc IN ('NZ', 'NZL', 'Chilli Bin', 'nz') THEN 'Auckland'
                                    WHEN dc IN ('GR', 'gr') THEN 'Banbury'
                                    WHEN dc IN ('Barleben', 'bx', 'BRN', 'BX') THEN 'Barleben'
                                    WHEN dc IN ('BL', 'NL', 'NL KP', 'sh') THEN 'Bleiswijk'
                                    WHEN dc IN ('SE', 'SK', 'SW', 'Bjuv') THEN 'Cloudberry'
                                    WHEN dc IN ('WF') THEN 'Co-Packer'
                                    WHEN dc IN ('TO') THEN 'Derby'
                                    WHEN dc IN ('AB', 'CAN AB') THEN 'Edmonton'
                                    WHEN dc IN ('JP') THEN 'Japan'
                                    WHEN dc IN ('CH') THEN 'Koelliken'
                                    WHEN dc IN ('FR', 'LI', 'Lisses', 'li', 'Lisse') THEN 'Lisses'
                                    WHEN dc IN ('Madrid') THEN 'Madrid'
                                    WHEN dc IN ('ML', 'AUS MEL', 'ml') THEN 'Melbourne'
                                    WHEN dc IN ('IT') THEN 'Milan'
                                    WHEN dc IN ('BV', 'BV ', 'bv') THEN 'Nuneaton'
                                    WHEN dc IN ('ON', 'On', 'CAN ON (T+S)', 'Toronto') THEN 'Ontario'
                                    WHEN dc IN ('MO', 'NO') THEN 'Oslo'
                                    WHEN dc IN ('PH', 'AUS PER', 'Casa', 'ph') THEN 'Perth'
                                    WHEN dc IN ('SY', 'AUS SYD', 'Esky', 'sy') THEN 'Sydney'
                                    WHEN dc IN ('BC', 'CAN BC') THEN 'Vancouver'
                                    WHEN dc IN ('AT', 'DE', 'VE', 've') THEN 'Verden'
                                    WHEN dc IN ('unmapped') THEN 'unassigned'
                                    ELSE dc END                                                           AS distribution_center,
                                hellofresh_week_where_error_happened                                      AS hellofresh_week_error,
                                CASE
                                    WHEN hellofresh_week_where_error_happened < '2023-W27' THEN 'H1'
                                    WHEN hellofresh_week_where_error_happened >= '2023-W27'
                                        THEN 'H2' END                                                     AS error_year_part,
                                hellofresh_month,
                                fk_customer,
                                mapped_accountable_team,
                                ROUND(compensation_amount_eur, 2)                                         AS compensation_amount_eur
                FROM materialized_views.cc_errors_processed
                WHERE hellofresh_year = '2023'
                  AND compensation_amount_eur > 0 AND compensation_amount_eur < 184
                  AND compound_metric_number <= 5
                  AND dc IN
                      ('NZ', 'GR', 'Barleben', 'BL', 'SK', 'Dublin', 'CH', 'LI', 'FR', 'Madrid', 'Melbourne', 'Milan',
                       'BV', 'MO', 'NO', 'Perth', 'Sydney', 'VE')
                  AND fk_customer != '-1'
                )
  --SELECT distribution_center,SUM(compensation_amount_eur) FROM VIEW_2 WHERE fk_customer='126016938' group by 1 /*

   , VIEW_3 AS (SELECT a.distribution_center,
                       a.fk_customer,
                       --COUNT(DISTINCT b.box_id)                                  AS box_count,
                       COUNT(DISTINCT a.box_id)                                  AS error_box_count,
                       ROUND(SUM(a.compensation_amount_eur), 2)                  AS compensation,
                       ROUND(COUNT(DISTINCT CASE
                           WHEN a.mapped_accountable_team = 'Production'
                               AND a.compensation_amount_eur > 0
                               THEN a.complaint_item_id END),2) AS Production,
                       ROUND(COUNT(DISTINCT CASE
                           WHEN a.mapped_accountable_team = 'Procurement'
                               AND a.compensation_amount_eur > 0
                               THEN a.complaint_item_id END),2) AS Procurement,
                       ROUND(COUNT(DISTINCT CASE
                           WHEN a.mapped_accountable_team = 'Logistics' AND a.compensation_amount_eur > 0
                               THEN a.complaint_item_id END),2) AS Logistics,
                       ROUND(SUM( CASE
                           WHEN a.mapped_accountable_team = 'Production'
                               AND a.compensation_amount_eur > 0
                               THEN a.compensation_amount_eur END),2) AS Prod_Comp,
                       ROUND(SUM( CASE
                           WHEN a.mapped_accountable_team = 'Procurement'
                               AND a.compensation_amount_eur > 0
                               THEN a.compensation_amount_eur END),2) AS Proc_Comp,
                       ROUND(SUM( CASE
                           WHEN a.mapped_accountable_team = 'Logistics' AND a.compensation_amount_eur > 0
                               THEN a.compensation_amount_eur END),2) AS Log_Comp
                FROM VIEW_2 AS a /*
                LEFT JOIN VIEW_1 AS b
                    ON a.fk_customer = b.fk_customer */
                GROUP BY 1,2--,3
)
 --        SELECT * FROM VIEW_2 WHERE fk_customer='127083890' /*


, VIEW_4 AS (SELECT a.distribution_center,
                    a.fk_customer,
                    COUNT(DISTINCT b.box_id) AS box_count,
                    a.error_box_count,
                    a.compensation,
                    a.Production,
                    a.Procurement,
                    a.Logistics,
                    a.Prod_Comp,
                    a.Proc_Comp,
                    a.Log_Comp,
                    ROW_NUMBER() OVER (PARTITION BY distribution_center ORDER BY compensation DESC) AS customer_rank
            FROM VIEW_3 AS a
            LEFT JOIN VIEW_1 AS b
                ON a.fk_customer = b.fk_customer
            WHERE error_box_count > 0
            GROUP BY 1,2,4,5,6,7,8,9,10,11
            ORDER BY compensation DESC)

   --SELECT * FROM VIEW_4 WHERE customer_rank <11 /*

, VIEW_5 AS (SELECT CASE
                        WHEN customer_rank <= 10 THEN 10
                        WHEN customer_rank <= 100 THEN 100
                        WHEN customer_rank <= 1000 THEN 1000
                        WHEN customer_rank <= 10000 THEN 10000
                        WHEN customer_rank <= 100000 THEN 100000
                        WHEN customer_rank <= 1000000 THEN 1000000
                        WHEN customer_rank <= 10000000 THEN 10000000
                        END                     AS top_x_column,
                    --COUNT(DISTINCT distribution_center) AS total_dc,
                    ROUND(SUM(compensation), 2) AS compensation,
                    SUM(box_count)              AS box_count,
                    SUM(error_box_count)        AS error_box_count,
                    ROUND(SUM(Logistics), 2)    AS Logistics,
                    ROUND(SUM(Production), 2)    AS Production,
                    ROUND(SUM(Procurement), 2)    AS Procurement,
                    ROUND(SUM(Log_Comp), 2)    AS Log_Comp,
                    ROUND(SUM(Prod_Comp), 2)    AS Prod_Comp,
                    ROUND(SUM(Proc_Comp), 2)    AS Proc_Comp
             FROM VIEW_4
             GROUP BY 1
             ORDER BY 1 ASC)

SELECT top_x_column,
       --total_dc,
       compensation,
       (1-(error_box_count/box_count)) AS perfect_box_rate,
       box_count,
       error_box_count,
       Logistics,
       Production,
       Procurement,
       Log_Comp,
       Prod_Comp,
       Proc_Comp
FROM VIEW_5
