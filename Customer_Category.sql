--- For the distribution of average compensation using the upper quartile (based on box count and total compensation) as the threshold
WITH VIEW_1 AS (
SELECT DISTINCT complaint_item_id,
         box_id,
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
       hellofresh_week_where_error_happened AS hellofresh_week,
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
  AND fk_customer NOT IN ('-1')
)

, VIEW_2 AS (
    SELECT
        fk_customer,
        COUNT(DISTINCT box_id) AS box_count,
        SUM(compensation_amount_eur) AS total_compensation
    FROM
        VIEW_1
    GROUP BY
        fk_customer
)

, VIEW_3 AS (
SELECT
    fk_customer,
    box_count,
    total_compensation,
    CASE
        WHEN box_count > 2  AND total_compensation > 8.9 THEN 'High frequency, high compensation'
        WHEN box_count <= 2 AND total_compensation > 8.9 THEN 'Low frequency, high compensation'
        WHEN box_count > 2 AND total_compensation <= 8.9 THEN 'High frequency, low compensation'
        WHEN box_count <= 2 AND total_compensation <= 8.9 THEN 'Low frequency, low compensation'
        ELSE 'Other'
    END AS customer_category
    /*CASE
        WHEN box_count > (SELECT PERCENTILE(box_count,0.75) FROM VIEW_2) AND total_compensation > (SELECT PERCENTILE(compensation_amount_eur,0.75) FROM VIEW_1) THEN 'High frequency, high compensation'
        WHEN box_count <= (SELECT PERCENTILE(box_count,0.75) FROM VIEW_2) AND total_compensation > (SELECT PERCENTILE(compensation_amount_eur,0.75) FROM VIEW_1) THEN 'Low frequency, high compensation'
        WHEN box_count > (SELECT PERCENTILE(box_count,0.75) FROM VIEW_2) AND total_compensation <= (SELECT PERCENTILE(compensation_amount_eur,0.75) FROM VIEW_1) THEN 'High frequency, low compensation'
        WHEN box_count <= (SELECT PERCENTILE(box_count,0.75) FROM VIEW_2) AND total_compensation <= (SELECT PERCENTILE(compensation_amount_eur,0.75) FROM VIEW_1) THEN 'Low frequency, low compensation'
        ELSE 'Other'
    END AS customer_category */
FROM VIEW_2
)

SELECT b.customer_category,
       --a.mapped_error_subcategory,
       COUNT(DISTINCT a.fk_customer) as customer_count,
       COUNT(DISTINCT a.box_id) as box_count,
       SUM(a.compensation_amount_eur) as total_compensation
FROM VIEW_1 AS a
LEFT JOIN VIEW_3 AS b
    ON a.fk_customer = b.fk_customer
GROUP BY 1
