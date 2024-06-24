WITH VIEW_1 AS (
    SELECT DISTINCT a.mapped_dc,
        a.fk_customer,
        MIN(a.hellofresh_week) AS start_week
/*        a.hellofresh_week,
        CASE
            WHEN a.hellofresh_week < '2023-W27' THEN 'H1'
            WHEN a.hellofresh_week >= '2023-W27' THEN 'H2'
        END AS box_year_part,
        a.box_id,
        COUNT(DISTINCT b.ingredient_name) AS meal_kits */
    FROM materialized_views.box_production_dimensions AS a
    WHERE a.fk_customer <> '-1'
        AND a.mapped_dc IN ('Auckland', 'Banbury', 'Barleben', 'Bjuv', 'Dublin', 'Koelliken', 'Lisses', 'Madrid', 'Melbourne', 'Milan', 'Nuneaton', 'Oslo', 'Perth', 'Prismalaan', 'Sydney', 'Verden')
    GROUP BY 1,2--,3,4,5
)

, VIEW_2 AS (
    SELECT DISTINCT a.mapped_dc,
                    a.fk_customer,
                    a.start_week,
                    b.hellofresh_week,
                    b.box_id,
                    COUNT(DISTINCT c.ingredient_name) AS meal_kits
    FROM VIEW_1 AS a
    LEFT JOIN materialized_views.box_production_dimensions AS b
       ON a.mapped_dc = b.mapped_dc
       AND a.fk_customer = b.fk_customer
       AND a.start_week = b.hellofresh_week
    LEFT JOIN materialized_views.pick_production_dimensions AS c
        ON a.mapped_dc = c.mapped_dc
        AND a.start_week = c.hellofresh_week
        AND b.box_id = c.box_id
    WHERE a.fk_customer <> '-1'
        AND a.mapped_dc IN ('Auckland', 'Banbury', 'Barleben', 'Bjuv', 'Dublin', 'Koelliken', 'Lisses', 'Madrid', 'Melbourne', 'Milan', 'Nuneaton', 'Oslo', 'Perth', 'Prismalaan', 'Sydney', 'Verden')
        AND (
            LOWER(c.ingredient_name) LIKE '%kit%'
            OR LOWER(c.ingredient_sku) LIKE '%kit%'
            OR LOWER(c.ingredient_name) LIKE '%mk%'
            OR c.ingredient_name LIKE '%-r'
        )
        AND a.start_week>='2023-W01' AND a.start_week<='2023-W52'
    GROUP BY 1,2,3,4,5
    ORDER BY 6 DESC
)


, VIEW_3 AS (
    SELECT (CASE WHEN dc IN ('NZ','NZL','Chilli Bin','nz') THEN 'Auckland'
                    WHEN dc IN ('GR','gr') THEN 'Banbury'
                    WHEN dc IN ('Barleben','bx','BRN','BX') THEN 'Barleben'
                    WHEN dc IN ('BL','NL','NL KP','sh') THEN 'Prismalaan'
                    WHEN dc IN ('SE','SK','SW','Bjuv') THEN 'Bjuv'
                    WHEN dc IN ('CH') THEN 'Koelliken'
                    WHEN dc IN ('FR','LI','Lisses','li','Lisse') THEN 'Lisses'
                    WHEN dc IN ('Madrid') THEN 'Madrid'
                    WHEN dc IN ('ML','AUS MEL','ml') THEN 'Melbourne'
                    WHEN dc IN ('IT') THEN 'Milan'
                    WHEN dc IN ('BV','BV ','bv') THEN 'Nuneaton'
                    WHEN dc IN ('MO','NO') THEN 'Oslo'
                    WHEN dc IN ('PH','AUS PER','Casa','ph') THEN 'Perth'
                    WHEN dc IN ('SY','AUS SYD','Esky','sy') THEN 'Sydney'
                    WHEN dc IN ('AT','DE','VE','ve') THEN 'Verden'
                    ELSE dc END) AS dc_name,
        hellofresh_week_where_error_happened,
        fk_customer,
        box_id,
        compensation_amount_eur AS compensation,
        MAX(CASE WHEN hellofresh_week_where_error_happened BETWEEN '2023-W01' AND '2023-W26' THEN 1 ELSE 0 END) AS H1_Error,
        MAX(CASE WHEN hellofresh_week_where_error_happened BETWEEN '2023-W27' AND '2023-W52' THEN 1 ELSE 0 END) AS H2_Error
    FROM materialized_views.cc_errors_processed
    WHERE hellofresh_year = '2023'
        AND compensation_amount_eur > 0
        AND compensation_amount_eur < 184
        AND compound_metric_number <= 5
        AND dc IN ('NZ', 'GR', 'Barleben', 'BL', 'SK', 'Dublin', 'CH', 'LI', 'FR', 'Madrid', 'Melbourne', 'Milan', 'BV', 'MO', 'NO', 'Perth', 'Sydney', 'VE')
        AND fk_customer != '-1'
    GROUP BY 1,2,3,4,5
    ORDER BY 1,2
)

, VIEW_4 AS (
SELECT a.mapped_dc,
       a.fk_customer,
       a.start_week,
       a.box_id,
       a.meal_kits,
       SUM(b.compensation) AS total_compensation
FROM VIEW_2 AS a
LEFT JOIN VIEW_3 AS b
    ON a.mapped_dc = b.dc_name
    AND a.fk_customer = b.fk_customer
    AND a.hellofresh_week = b.hellofresh_week_where_error_happened
    AND a.box_id = b.box_id
WHERE compensation > 0
GROUP BY 1,2,3,4,5--,6
ORDER BY 2
)

SELECT mapped_dc,meal_kits,SUM(total_compensation) AS total_compensation
FROM VIEW_4
GROUP BY 1,2
ORDER BY 1,2
