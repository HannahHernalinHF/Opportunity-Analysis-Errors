--- this query is to get the number of total cohorts A & B and these cohorts at 10 week retention by dimension (market, box size, & no. of meal kits)

--- start week per customer
WITH VIEW_1 AS (
    SELECT country_group AS market,
        fk_customer,
        MIN(hellofresh_week) AS start_week
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1'
          AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden') AND hellofresh_week >= "2023-W01" AND hellofresh_week <= "2023-W39"
    GROUP BY 1,2
)

--- week retention
, VIEW_2 AS (
SELECT DISTINCT a.market,
    a.fk_customer,
    a.start_week,
    b.box_size,
    COUNT(DISTINCT CASE WHEN (LOWER(c.ingredient_name) LIKE '%kit%'
        OR LOWER(c.ingredient_sku) LIKE '%kit%'
        OR LOWER(c.ingredient_name) LIKE '%mk%'
        OR c.ingredient_name LIKE '%-r') THEN c.ingredient_name ELSE 0 END) AS meal_kits,
    concat(a.market, " ",b.box_size,"P ",COUNT(DISTINCT CASE WHEN (LOWER(c.ingredient_name) LIKE '%kit%'
        OR LOWER(c.ingredient_sku) LIKE '%kit%'
        OR LOWER(c.ingredient_name) LIKE '%mk%'
        OR c.ingredient_name LIKE '%-r') THEN c.ingredient_name ELSE 0 END)," MK") AS dimension,
    CASE WHEN RIGHT(b.hellofresh_week, 2) = RIGHT(a.start_week, 2) THEN 0 ELSE RIGHT(b.hellofresh_week, 2) - RIGHT(a.start_week, 2) END AS week_retention
FROM VIEW_1 AS a
LEFT JOIN materialized_views.box_production_dimensions AS b
    ON a.fk_customer = b.fk_customer
    AND a.market = b.country_group
    --AND a.mapped_dc = b.mapped_dc
LEFT JOIN materialized_views.pick_production_dimensions AS c
    ON b.mapped_dc = c.mapped_dc
    AND b.box_id = c.box_id
WHERE RIGHT(b.hellofresh_week,2) >= RIGHT(a.start_week,2)
    AND a.start_week >= '2023-W01' AND a.start_week <= '2023-W39'
    AND b.box_size > 0
GROUP BY 1,2,3,4,b.hellofresh_week
ORDER BY 1,2,3,4
)


--- customers first week error
, VIEW_3 AS (
    SELECT country_group AS market,
           fk_customer,
           MIN(hellofresh_week_where_error_happened) AS start_error_week
    FROM materialized_views.cc_errors_processed
    WHERE hellofresh_year = '2023'
      AND compensation_amount_eur > 0 AND compensation_amount_eur < 184
      AND compound_metric_number <= 5
      AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
      AND fk_customer <> '-1'
    GROUP BY 1,2
    ORDER BY 1,2
)

--- customers with (cohort A) or without (cohort B) first box errors
, VIEW_4 AS (
SELECT DISTINCT a.market, --a.mapped_dc,
       concat(a.market, " ",a.box_size,"P ",a.meal_kits," MK") AS dimension,
       --a.box_size,
       a.fk_customer,
       CASE WHEN (a.start_week = b.start_error_week) IS NULL THEN false ELSE a.start_week = b.start_error_week END AS with_first_box_error
       --a.meal_kits
FROM VIEW_2 AS a
LEFT JOIN VIEW_3 AS b
    ON a.fk_customer = b.fk_customer
    AND a.market = b.market
    --AND a.mapped_dc = b.dc_name
WHERE a.start_week >= '2023-W01' AND a.week_retention>0 AND a.week_retention<11-- AND a.week_retention=10
ORDER BY 1,3
)


--- cohort A: total customers per DC regardless of week retention
, VIEW_5 AS (
    SELECT --a.market,
        --a.box_size,
        --a.meal_kits,
        concat(a.market, " ",a.box_size,"P ",a.meal_kits," MK") AS dimension,
        --a.week_retention,
        COUNT(DISTINCT CASE WHEN with_first_box_error = true THEN a.fk_customer END) AS cohort_A,
        COUNT(DISTINCT CASE WHEN with_first_box_error = true AND a.week_retention=10 THEN a.fk_customer END) AS cohort_A_10wk,
        COUNT(DISTINCT CASE WHEN with_first_box_error = true AND a.week_retention=10 THEN a.fk_customer END)/COUNT(DISTINCT CASE WHEN with_first_box_error = true THEN a.fk_customer END) AS retention_A,
        LEAST(COUNT(DISTINCT CASE WHEN with_first_box_error = true THEN a.fk_customer END),COUNT(DISTINCT CASE WHEN with_first_box_error = false OR with_first_box_error IS NULL THEN a.fk_customer END)) AS min_cohort_size--new_cohort_B
        --COUNT(DISTINCT CASE WHEN with_first_box_error = true AND a.week_retention=10 THEN a.fk_customer END)/COUNT(DISTINCT CASE WHEN with_first_box_error = true THEN a.fk_customer END) AS retention_A
    FROM VIEW_2 AS a
    JOIN VIEW_4 AS b
        ON a.fk_customer = b.fk_customer
        AND a.market = b.market
        --AND a.mapped_dc = b.mapped_dc
    WHERE a.week_retention>0 AND a.week_retention < 11 AND a.meal_kits<11--AND a.meal_kits>0
    GROUP BY 1
    ORDER BY 1
)


, VIEW_6 AS (
    SELECT
        a.fk_customer,
        a.dimension,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY a.dimension ORDER BY RAND()) <= c.min_cohort_size THEN ROW_NUMBER() OVER (PARTITION BY a.dimension ORDER BY RAND())
            ELSE 0 END AS random_rank_10wk
    FROM VIEW_2 AS a
    LEFT JOIN VIEW_4 AS b
        ON a.market=b.market
        AND a.dimension=b.dimension
        AND a.fk_customer=b.fk_customer
    JOIN VIEW_5 AS c
        ON a.dimension = c.dimension
    WHERE b.with_first_box_error=false AND a.week_retention=10
    GROUP BY 1,2,min_cohort_size
)

   --SELECT * FROM VIEW_6 WHERE dimension='AU 2P 1 MK' AND random_rank_10wk>0 ORDER BY 2,3 LIMIT 500/*

, VIEW_7 AS (
    SELECT
        a.dimension,
        a.min_cohort_size AS cohort_B,
        --SUM(CASE WHEN b.random_rank > 0 THEN 1 ELSE 0 END) AS cohort_B,
        SUM(CASE WHEN c.random_rank_10wk > 0 AND c.random_rank_10wk <= min_cohort_size THEN 1 ELSE 0 END) AS cohort_B_10wk,
        SUM(CASE WHEN c.random_rank_10wk > 0 AND c.random_rank_10wk <= min_cohort_size THEN 1 ELSE 0 END)/a.min_cohort_size AS retention_B
    FROM VIEW_5 AS a
    --LEFT JOIN VIEW_7 AS b
    --    ON a.dimension=b.dimension
    LEFT JOIN VIEW_6 AS c
        ON a.dimension=c.dimension
    GROUP BY 1,2
    ORDER BY 1,2
)

SELECT a.dimension,
       a.cohort_A,
       a.cohort_A_10wk,
       a.retention_A,
       b.cohort_B,
       b.cohort_B_10wk,
       B.retention_B
FROM VIEW_5 AS a
LEFT JOIN VIEW_7 AS b
    ON a.dimension=b.dimension
WHERE cohort_A>30
ORDER BY 1
