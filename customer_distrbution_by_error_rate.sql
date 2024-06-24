WITH VIEW_1 AS (
SELECT fk_customer,
       COUNT(DISTINCT box_id) AS error_boxes,
       SUM(compensation_amount_eur) AS total_compensation
FROM materialized_views.cc_errors_processed
WHERE hellofresh_year='2023'
  AND compensation_amount_eur>0
  AND compound_metric_number<=5
  AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
  AND fk_customer NOT IN ('-1')
GROUP BY 1
)

, VIEW_2 AS (
SELECT fk_customer,
       COUNT(DISTINCT box_id) AS total_boxes
FROM materialized_views.box_production_dimensions
WHERE hellofresh_week >= '2023-W01'
  AND fk_customer <> '-1'
  AND mapped_dc IN  ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
GROUP BY 1
)

, VIEW_3 AS (
SELECT b.fk_customer,
       SUM(a.total_compensation) AS total_compensation,
       SUM(b.total_boxes) AS total_boxes,
       SUM(a.error_boxes) AS error_boxes
FROM VIEW_1 AS a
FULL JOIN VIEW_2 AS b
    ON a.fk_customer = b.fk_customer
WHERE (error_boxes>0 AND error_boxes IS NOT NULL)
GROUP BY 1
)

, VIEW_4 AS (
SELECT fk_customer,
       SUM(total_boxes) AS total_boxes,
       SUM(error_boxes) AS error_boxes,
       ROUND(SUM(error_boxes/total_boxes),2) AS error_rate,
       ROUND(SUM(total_compensation),2) AS total_compensation
FROM VIEW_3
GROUP BY 1
)


SELECT error_rate,
       total_boxes,
       error_boxes,
       count(distinct fk_customer) AS customer_count
FROM VIEW_4
WHERE error_rate <= 1.0
GROUP BY 1,2,3
ORDER BY 1 DESC
