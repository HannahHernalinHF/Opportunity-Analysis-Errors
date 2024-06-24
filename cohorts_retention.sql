--- start week per customer
WITH VIEW_1 AS (
    SELECT
        mapped_dc AS dc,
        fk_customer,
        MIN(hellofresh_week) AS start_week
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1'
          AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
    GROUP BY 1,2
)

--- week retention
, VIEW_2 AS (
SELECT
    a.dc,
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
SELECT a.dc,
       a.fk_customer,
       a.start_week = b.start_error_week AS with_first_box_error
FROM VIEW_2 AS a
FULL JOIN VIEW_5 AS b
    ON a.fk_customer = b.fk_customer
WHERE a.start_week >= '2023-W01'
)

 , VIEW_7 AS (
SELECT dc,
       week_retention,
       cohort_A,
       cohort_B,
       total_cust_cohortA_dc,
       total_cust_cohortB_dc
FROM (
    SELECT a.dc,
           a.week_retention,
           COUNT(DISTINCT CASE WHEN b.with_first_box_error=true THEN a.fk_customer END) AS cohort_A,
           COUNT(DISTINCT CASE WHEN b.with_first_box_error=false OR b.with_first_box_error IS NULL THEN a.fk_customer END) AS cohort_B,
           MAX(COUNT(DISTINCT CASE WHEN b.with_first_box_error=true THEN a.fk_customer END)) OVER (PARTITION BY a.dc) AS total_cust_cohortA_dc,
           MAX(COUNT(DISTINCT CASE WHEN b.with_first_box_error=false OR b.with_first_box_error IS NULL THEN a.fk_customer END)) OVER (PARTITION BY a.dc) AS total_cust_cohortB_dc
    FROM VIEW_2 AS a
    JOIN VIEW_6 AS b
        ON a.fk_customer=b.fk_customer
    GROUP BY a.dc, a.week_retention
) subquery
ORDER BY dc, week_retention
)

SELECT dc,
       week_retention,
       cohort_A,
       cohort_B,
       --total_cust_cohortA_dc,
       --total_cust_cohortB_dc,
       cohort_A/total_cust_cohortA_dc AS retention_A,
       cohort_B/total_cust_cohortB_dc AS retention_B
FROM VIEW_7
WHERE week_retention>0 AND week_retention<11
