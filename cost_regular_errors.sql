--- to calculate the cost of sending boxes with errors regularly

WITH VIEW_2 AS (
SELECT fk_customer,
       COUNT(DISTINCT box_id) AS total_boxes,
       COUNT(DISTINCT hellofresh_week) AS weeks
FROM materialized_views.box_production_dimensions
WHERE hellofresh_week LIKE '2023%'
  AND fk_customer <> '-1'
  AND mapped_dc IN  ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
GROUP BY 1
ORDER BY 3 DESC
)

SELECT weeks,
       SUM(total_boxes),
       COUNT(DISTINCT fk_customer)
FROM VIEW_2
GROUP BY 1
ORDER BY 1
