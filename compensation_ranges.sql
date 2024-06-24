WITH VIEW_1 AS (
  SELECT
    --fk_customer,
    compensation_amount_eur,
    complaint_item_id,
    CASE
      WHEN compensation_amount_eur <= 10 THEN '0-10'
      WHEN compensation_amount_eur <= 20 THEN '11-20'
      WHEN compensation_amount_eur <= 30 THEN '21-30'
      ELSE '31+'
    END AS compensation_range
  FROM materialized_views.cc_errors_processed
  WHERE hellofresh_year = '2023'
    AND compensation_amount_eur > 0
    AND compound_metric_number <= 5
    AND dc IN ('NZ', 'GR', 'Barleben', 'BL', 'SK', 'Dublin', 'CH', 'LI', 'FR', 'Madrid', 'Melbourne', 'Milan', 'BV', 'MO', 'NO', 'Perth', 'Sydney', 'VE')
    AND fk_customer NOT IN ('-1')
)

SELECT
  compensation_range,
  --COUNT(DISTINCT fk_customer) AS customer_count,
  COUNT(DISTINCT complaint_item_id) AS errors,
  ROUND(SUM(compensation_amount_eur), 2) AS total_compensation
FROM VIEW_1
GROUP BY 1
ORDER BY 1
