--- start week per customer
WITH VIEW_1 AS (
    SELECT
        fk_customer,
        MIN(hellofresh_week) AS start_week
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1'
          AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
    GROUP BY 1
)

--- week retention
, VIEW_2 AS (
SELECT
    a.fk_customer,
    a.start_week,
    b.hellofresh_week AS following_weeks,
    CASE WHEN RIGHT(b.hellofresh_week, 2) = RIGHT(a.start_week, 2) THEN 0 ELSE RIGHT(b.hellofresh_week, 2) - RIGHT(a.start_week, 2) END AS week_retention
FROM VIEW_1 a
JOIN materialized_views.box_production_dimensions b
    ON a.fk_customer = b.fk_customer
WHERE RIGHT(b.hellofresh_week, 2) >= RIGHT(a.start_week, 2)
      AND a.start_week >= '2023-W01' AND a.start_week <= '2023-W39'
ORDER BY a.fk_customer, a.start_week, b.hellofresh_week
)
/*
--- box count per customer, week
, VIEW_3 AS (
    SELECT
        fk_customer,
        hellofresh_week,
        COUNT(DISTINCT box_id) AS box_count
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1'
          AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
          AND hellofresh_week >='2023-W01' AND hellofresh_week <='2023-W39'
    GROUP BY 1,2
    ORDER BY 1,2
)

, VIEW_4 AS (
    SELECT fk_customer,
           hellofresh_week_where_error_happened AS error_week,
           box_id AS error_box_id,
           COUNT(DISTINCT box_id) AS error_box_count,
           ROUND(SUM(compensation_amount_eur),2) AS compensation
    FROM materialized_views.cc_errors_processed
    WHERE hellofresh_year = '2023'
      AND compensation_amount_eur > 0 AND compensation_amount_eur < 184
      AND compound_metric_number <= 5
      AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
      AND fk_customer != '-1'
    GROUP BY 1,2,3
    ORDER BY 1,2,3
)
*/
--- customers first week error
, VIEW_5 AS (
    SELECT fk_customer,
           MIN(hellofresh_week_where_error_happened) AS start_error_week
    FROM materialized_views.cc_errors_processed
    WHERE hellofresh_year = '2023'
      AND compensation_amount_eur > 0 AND compensation_amount_eur < 184
      AND compound_metric_number <= 5
      AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
      AND fk_customer != '-1'
    GROUP BY 1
    ORDER BY 1
)

--- customers with or without first box errors
, VIEW_6 AS (
SELECT a.fk_customer,
       a.start_week = b.start_error_week AS with_first_box_error
FROM VIEW_2 AS a
FULL JOIN VIEW_5 AS b
    ON a.fk_customer = b.fk_customer
)


SELECT COUNT(DISTINCT CASE WHEN a.with_first_box_error=true THEN a.fk_customer END) AS total_customers_with_fbe,
       COUNT(DISTINCT CASE WHEN a.with_first_box_error=false THEN a.fk_customer END) AS total_customers_without_fbe,
       COUNT(DISTINCT CASE WHEN b.week_retention = 5 AND a.with_first_box_error=true THEN a.fk_customer END) AS 5Wfirst_box_errors,
       COUNT(DISTINCT CASE WHEN b.week_retention = 5 AND a.with_first_box_error=false THEN a.fk_customer END) AS 5Wno_first_box_errors,
       COUNT(DISTINCT CASE WHEN b.week_retention = 10 AND a.with_first_box_error=true THEN a.fk_customer END) AS 10Wfirst_box_errors,
       COUNT(DISTINCT CASE WHEN b.week_retention = 10 AND a.with_first_box_error=false THEN a.fk_customer END) AS 10Wno_first_box_errors
FROM VIEW_6 AS a
JOIN VIEW_2 AS b
    ON a.fk_customer=b.fk_customer
