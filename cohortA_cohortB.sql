---- to get the list of customers who had first box errors and no first box errors


WITH CustomerFirstOrder AS (
    SELECT fk_customer,
           MIN(hellofresh_week) AS first_order_week,
           COUNT(DISTINCT hellofresh_week) AS total_weeks
    FROM materialized_views.box_production_dimensions
    WHERE fk_customer <> '-1'
      AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
    GROUP BY fk_customer
)

, Errors AS (
  SELECT fk_customer AS error_fk_customer,
         --box_id AS error_box_id,
         MIN(hellofresh_week_where_error_happened) AS start_error_week
  FROM materialized_views.cc_errors_processed
  WHERE hellofresh_year = '2023'
    AND compensation_amount_eur > 0
    AND compound_metric_number <= 5
    AND dc IN ('NZ', 'GR', 'Barleben', 'BL', 'SK', 'Dublin', 'CH', 'LI', 'FR', 'Madrid', 'Melbourne', 'Milan', 'BV', 'MO', 'NO', 'Perth', 'Sydney', 'VE')
    AND fk_customer NOT IN ('-1')
  GROUP BY 1
)

, FirstBoxError AS (
SELECT DISTINCT a.fk_customer AS new_customer,
       a.hellofresh_week AS First_Box_Week,
       b.hellofresh_month,
       d.start_error_week,
       CASE WHEN c.first_order_week = d.start_error_week THEN "Y" ELSE "N" END AS First_Box_With_Error
FROM materialized_views.box_production_dimensions AS a
LEFT JOIN dimensions.date_dimension AS b
    ON a.hellofresh_week = b.hellofresh_week
JOIN CustomerFirstOrder AS c
    ON a.fk_customer = c.fk_customer
LEFT JOIN Errors AS d
    ON a.fk_customer = d.error_fk_customer
WHERE a.fk_customer <> '-1'
  AND a.mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
  AND b.hellofresh_year = '2023'
  AND b.hellofresh_month < 7
  AND a.hellofresh_week = c.first_order_week
)

, Retention AS (
SELECT a.First_Box_With_Error,
       a.new_customer,
       a.First_Box_Week,
       MIN(b.hellofresh_week) AS start_week,
       SUM(CASE WHEN b.hellofresh_week>a.First_Box_Week THEN 1 ELSE 0 END) AS total_weeks
FROM FirstBoxError AS a
LEFT JOIN  materialized_views.box_production_dimensions AS b
    ON a.new_customer=b.fk_customer
GROUP BY 1,2,3
ORDER BY 2
)

SELECT DISTINCT First_Box_With_Error,
       new_customer,
       First_Box_Week,
       total_weeks,
       CASE WHEN total_weeks >= 5 THEN 1 ELSE 0 END AS 5_week_retention,
       CASE WHEN total_weeks >= 10 THEN 1 ELSE 0 END AS 10_week_retention
FROM Retention
ORDER BY 2,1
