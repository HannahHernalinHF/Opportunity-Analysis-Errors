--- to get the error rate per customer id and box size 
WITH VIEW_1 AS (
SELECT INT(box_size) AS box_size,
       fk_customer,
       --box_id AS error_box_id
       --COUNT(DISTINCT hellofresh_week_where_error_happened) AS error_weeks,
       COUNT(DISTINCT box_id) AS error_boxes
       --SUM(compensation_amount_eur) AS total_compensation
FROM materialized_views.cc_errors_processed
WHERE hellofresh_year='2023'
  AND compensation_amount_eur>0
  AND compound_metric_number<=5
  AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
  AND fk_customer NOT IN ('-1')
GROUP BY 1,2
)

, VIEW_2 AS (
SELECT INT(box_size) AS box_size,
       fk_customer,
       --box_id
       COUNT(DISTINCT box_id) AS total_boxes
       --COUNT(DISTINCT hellofresh_week) AS weeks
FROM materialized_views.box_production_dimensions
WHERE hellofresh_week LIKE '2023%'
  AND fk_customer <> '-1'
  AND mapped_dc IN  ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
GROUP BY 1,2
)

, VIEW_3 AS (
SELECT a.box_size,
       --a.fk_customer AS customer,
       ROUND(error_boxes/total_boxes,2) AS error_rate
FROM VIEW_1 AS a
LEFT JOIN VIEW_2 AS b
    ON a.fk_customer = b.fk_customer
ORDER BY 1,2,3
)

SELECT *
FROM VIEW_3
WHERE error_rate>0 AND box_size>0
ORDER BY 1 ASC
