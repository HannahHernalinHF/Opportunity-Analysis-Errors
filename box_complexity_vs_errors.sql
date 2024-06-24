WITH VIEW_1 AS (
SELECT DISTINCT complaint_item_id,
         box_id,
         box_size,
         country_group,
           CASE WHEN dc IN ('NZ','NZL','Chilli Bin','nz') THEN 'Auckland'
                    WHEN dc IN ('GR','gr') THEN 'Banbury'
                    WHEN dc IN ('Barleben','bx','BRN','BX') THEN 'Barleben'
                    WHEN dc IN ('BL','NL','NL KP','sh') THEN 'Bleiswijk'
                    WHEN dc IN ('SE','SK','SW','Bjuv') THEN 'Cloudberry'
                    WHEN dc IN ('WF') THEN 'Co-Packer'
                    WHEN dc IN ('TO') THEN 'Derby'
                    WHEN dc IN ('AB','CAN AB') THEN 'Edmonton'
                    WHEN dc IN ('JP') THEN 'Japan'
                    WHEN dc IN ('CH') THEN 'Koelliken'
                    WHEN dc IN ('FR','LI','Lisses','li','Lisse') THEN 'Lisses'
                    WHEN dc IN ('Madrid') THEN 'Madrid'
                    WHEN dc IN ('ML','AUS MEL','ml') THEN 'Melbourne'
                    WHEN dc IN ('IT') THEN 'Milan'
                    WHEN dc IN ('BV','BV ','bv') THEN 'Nuneaton'
                    WHEN dc IN ('ON','On','CAN ON (T+S)','Toronto') THEN 'Ontario'
                    WHEN dc IN ('MO','NO') THEN 'Oslo'
                    WHEN dc IN ('PH','AUS PER','Casa','ph') THEN 'Perth'
                    WHEN dc IN ('SY','AUS SYD','Esky','sy') THEN 'Sydney'
                    WHEN dc IN ('BC','CAN BC') THEN 'Vancouver'
                    WHEN dc IN ('AT','DE','VE','ve') THEN 'Verden'
                    WHEN dc IN ('unmapped') THEN 'unassigned'
                    ELSE dc END AS distribution_center,
       hellofresh_week_where_error_happened AS hellofresh_week_error,
       CASE WHEN hellofresh_week_where_error_happened <'2023-W27' THEN 'H1'
           WHEN hellofresh_week_where_error_happened >= '2023-W27' THEN 'H2' END AS error_year_part,
       hellofresh_month,
       fk_customer,
       mapped_accountable_team,
       mapped_error_category,
       mapped_error_subcategory,
       mapped_complaint,
       ingredient_subcategory,
       courier,
       delivery_region,
       ROUND(compensation_amount_eur,2) AS compensation_amount_eur
FROM materialized_views.cc_errors_processed
WHERE hellofresh_year='2023'
  AND compensation_amount_eur>0
  AND compound_metric_number<=5
  AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
  AND fk_customer != '-1'
)

SELECT box_size,
       COUNT(DISTINCT complaint_item_id) as errors,
       COUNT(fk_customer) as customers,
       SUM(compensation_amount_eur) as compensation
FROM VIEW_1
GROUP BY 1
