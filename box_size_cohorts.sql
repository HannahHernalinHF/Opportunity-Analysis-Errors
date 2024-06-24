--- start week per customer
WITH VIEW_1 AS (
    SELECT
        /*CASE WHEN country_group = 'BENELUX' THEN 'BNL'
            WHEN country_group = 'Nordics' THEN 'NORDICS'
            WHEN country_group = 'Ireland' THEN 'IE'
            WHEN country_group = 'Spain' THEN 'ES'
            ELSE country_group END AS market, */
        /*(CASE WHEN mapped_dc IN ('NZ','NZL','Chilli Bin','nz') THEN 'Auckland'
                    WHEN mapped_dc IN ('GR','gr') THEN 'Banbury'
                    WHEN mapped_dc IN ('Barleben','bx','BRN','BX') THEN 'Barleben'
                    WHEN mapped_dc IN ('BL','NL','NL KP','sh') THEN 'Prismalaan'
                    WHEN mapped_dc IN ('SE','SK','SW','Bjuv') THEN 'Bjuv'
                    WHEN mapped_dc IN ('CH') THEN 'Koelliken'
                    WHEN mapped_dc IN ('FR','LI','Lisses','li','Lisse') THEN 'Lisses'
                    WHEN mapped_dc IN ('Madrid') THEN 'Madrid'
                    WHEN mapped_dc IN ('ML','AUS MEL','ml') THEN 'Melbourne'
                    WHEN mapped_dc IN ('IT') THEN 'Milan'
                    WHEN mapped_dc IN ('BV','BV ','bv') THEN 'Nuneaton'
                    WHEN mapped_dc IN ('MO','NO') THEN 'Oslo'
                    WHEN mapped_dc IN ('PH','AUS PER','Casa','ph') THEN 'Perth'
                    WHEN mapped_dc IN ('SY','AUS SYD','Esky','sy') THEN 'Sydney'
                    WHEN mapped_dc IN ('AT','DE','VE','ve') THEN 'Verden'
                    ELSE mapped_dc END) AS dc_name, */
        fk_customer,
        --box_size,
        MIN(hellofresh_week) AS start_week
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1'
          AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
          --AND hellofresh_week >= '2023-W01' AND hellofresh_week <= '2023-W39'
    GROUP BY 1
)

--- week retention
, VIEW_2 AS (
SELECT DISTINCT
    a.fk_customer,
    a.start_week,
    b.box_size
    --CASE WHEN RIGHT(b.hellofresh_week, 2) = RIGHT(a.start_week, 2) THEN 0 ELSE RIGHT(b.hellofresh_week, 2) - RIGHT(a.start_week, 2) END AS week_retention
FROM VIEW_1 AS a
LEFT JOIN materialized_views.box_production_dimensions b
    ON a.fk_customer = b.fk_customer
    AND a.start_week = b.hellofresh_week
WHERE RIGHT(b.hellofresh_week, 2) >= RIGHT(a.start_week, 2)
      AND a.start_week >= '2023-W01' AND a.start_week <= '2023-W39'
ORDER BY a.fk_customer, a.start_week--, b.hellofresh_week
)

--SELECT fk_customer, count(distinct start_week) FROM VIEW_2 GROUP BY 1 HAVING count(distinct fk_customer)>1/*

--- customers first week error
, VIEW_3 AS (
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

--- new customers with or without first box errors
, VIEW_4 AS (
SELECT a.box_size,
       a.fk_customer,
       a.start_week = b.start_error_week AS with_first_box_error
FROM VIEW_2 AS a
FULL JOIN VIEW_3 AS b
    ON a.fk_customer = b.fk_customer
WHERE a.start_week >= '2023-W01'
)

, VIEW_5 AS (
--- count the number of customers per week retention for both cohorts: cohort A = 5 week retention customers, cohort B = 10 week retention customers
SELECT a.box_size,
       --a.week_retention,
       COUNT(DISTINCT CASE WHEN b.with_first_box_error=true THEN a.fk_customer END) AS cohort_A,
       --COUNT(DISTINCT CASE WHEN b.with_first_box_error=true THEN a.fk_customer END) / (SELECT COUNT(DISTINCT fk_customer) FROM VIEW_6 WHERE with_first_box_error=true) AS retention_A,
       COUNT(DISTINCT CASE WHEN b.with_first_box_error=false OR b.with_first_box_error IS NULL THEN a.fk_customer END) AS cohort_B
       --COUNT(DISTINCT CASE WHEN b.with_first_box_error=false OR b.with_first_box_error IS NULL THEN a.fk_customer END) / (SELECT COUNT(DISTINCT fk_customer) FROM VIEW_6 WHERE with_first_box_error=false OR with_first_box_error IS NULL) AS retention_B
FROM VIEW_2 AS a
JOIN VIEW_4 AS b
    ON a.fk_customer=b.fk_customer
GROUP BY 1--,2
ORDER BY 1--,2
)


SELECT * FROM VIEW_5
