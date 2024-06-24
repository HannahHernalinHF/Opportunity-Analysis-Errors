-- We want 2023 customer counts for: total customers, existing customers, new customers, new customers w/ error in first box, new customers w/o error in first box

-- Total 2023 customers
WITH VIEW_1 AS (
    SELECT
        fk_customer
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1' AND hellofresh_week <= '2023-W39'
    AND hellofresh_week>='2023-W01'
    AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
    GROUP BY 1
)

-- Customer start week
, VIEW_1A AS (
    SELECT  A.fk_customer
        , MIN(hellofresh_week) AS start_week
    FROM materialized_views.box_production_dimensions AS A
    INNER JOIN VIEW_1 AS B
    ON A.fk_customer=B.fk_customer
    WHERE A.fk_customer <> '-1' AND hellofresh_week <= '2023-W39'
    AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
    GROUP BY 1
)

--- week retention
, VIEW_2 AS (
    SELECT
        a.fk_customer,
        a.start_week,
        b.hellofresh_week AS following_weeks,
        (CASE WHEN RIGHT(b.hellofresh_week, 2) = RIGHT(a.start_week, 2) THEN 0 ELSE RIGHT(b.hellofresh_week, 2) - RIGHT(a.start_week, 2) END) AS week_retention
    FROM VIEW_1A a
    LEFT JOIN materialized_views.box_production_dimensions b
    ON a.fk_customer = b.fk_customer
    WHERE RIGHT(b.hellofresh_week, 2) >= RIGHT(a.start_week, 2)
    --AND a.start_week >= '2023-W01' AND a.start_week <= '2023-W39'
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
    SELECT a.*
        , b.start_error_week
        , (CASE WHEN a.start_week = b.start_error_week THEN 1 ELSE 0 END) AS with_first_box_error
    FROM VIEW_2 AS a
    LEFT JOIN VIEW_5 AS b
    ON a.fk_customer = b.fk_customer
)

SELECT COUNT(DISTINCT(fk_customer)) AS Total_Customers
    , COUNT(DISTINCT(CASE WHEN start_week>='2023-W01' THEN fk_customer ELSE NULL END)) AS New_Customers
    , COUNT(DISTINCT(CASE WHEN start_week<'2023-W01' THEN fk_customer ELSE NULL END)) AS Existing_Customers
    , COUNT(DISTINCT(CASE WHEN start_week>='2023-W01' AND with_first_box_error=1 THEN fk_customer ELSE NULL END)) AS FBE_Customers
    , COUNT(DISTINCT(CASE WHEN start_week>='2023-W01' AND with_first_box_error=0 THEN fk_customer ELSE NULL END)) AS FBNE_Customers
FROM VIEW_6
