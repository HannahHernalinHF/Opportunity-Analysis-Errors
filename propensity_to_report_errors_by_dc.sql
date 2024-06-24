WITH VIEW_1 AS (
SELECT DISTINCT mapped_dc,
       fk_customer,
       hellofresh_week,
       CASE WHEN hellofresh_week <'2023-W27' THEN 'H1'
           WHEN hellofresh_week >= '2023-W27' THEN 'H2' END AS box_year_part,
       box_id
FROM materialized_views.box_production_dimensions
WHERE hellofresh_week LIKE '2023%'
  AND fk_customer <> '-1'
  AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
)


, VIEW_2 AS (
SELECT mapped_dc,
       fk_customer,
       --box_year_part,
       COUNT(DISTINCT CASE WHEN box_year_part = 'H1' THEN box_id END) AS H1_count,
       COUNT(DISTINCT CASE WHEN box_year_part = 'H2' THEN box_id END) AS H2_count
FROM VIEW_1
GROUP BY 1,2
ORDER BY 1,2
)

   --SELECT * FROM VIEW_2 ORDER BY 2,3 DESC/*

, VIEW_3 AS (
SELECT DISTINCT (CASE WHEN dc IN ('NZ','NZL','Chilli Bin','nz') THEN 'Auckland'
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
                    ELSE dc END) AS dc_name
       , fk_customer
       , MAX(CASE WHEN hellofresh_week_where_error_happened BETWEEN '2023-W01' AND '2023-W26' THEN 1 ELSE 0 END) AS H1_Error
       , MAX(CASE WHEN hellofresh_week_where_error_happened BETWEEN '2023-W27' AND '2023-W52' THEN 1 ELSE 0 END) AS H2_Error
       --ROUND(compensation_amount_eur,2) AS compensation_amount_eur
FROM materialized_views.cc_errors_processed
WHERE hellofresh_year='2023'
AND compensation_amount_eur>0 AND compensation_amount_eur<184
AND compound_metric_number<=5
AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
AND fk_customer != '-1'
GROUP BY 1,2
ORDER BY 1,2
)

, VIEW_4 AS (
SELECT a.mapped_dc,
       a.H1_count,
       a.H2_count,
       concat(a.H1_count,"-",a.H2_count) AS box_perm,
       COUNT(DISTINCT CASE WHEN H1_Error=1 THEN b.fk_customer END) AS H1_error_customers,
       COUNT(DISTINCT CASE WHEN H1_Error=0 THEN b.fk_customer END) AS H1_no_error_customers,
       LEAST(COUNT(DISTINCT CASE WHEN H1_Error=1 THEN b.fk_customer END), COUNT(DISTINCT CASE WHEN H1_Error=0 THEN b.fk_customer END)) AS min_customer_count
FROM VIEW_2 AS a
LEFT JOIN VIEW_3 AS b
    ON a.fk_customer = b.fk_customer
    AND a.mapped_dc = b.dc_name
WHERE H1_count > 0 AND H2_count > 0 AND  H1_count < 27 AND H2_count < 27
GROUP BY 1,2,3
ORDER BY 1,2,3
)

, VIEW_5 AS (
    SELECT A.*
        , C.box_perm
        , SUBSTR(A.fk_customer,3,4) AS Rand_Str
        , COALESCE(B.H1_Error,0) AS H1_Error
        , COALESCE(B.H2_Error,0) AS H2_Error
        , C.min_customer_count
    FROM VIEW_2 AS A
    LEFT JOIN VIEW_3 AS B
    ON A.fk_customer=B.fk_customer
    LEFT JOIN VIEW_4 AS C
    ON A.H1_count=C.H1_count
    AND A.H2_count=C.H2_count)

, VIEW_6 AS (
SELECT mapped_dc
    , box_perm
    , H1_Error
    , COUNT(*) AS customer_count
    , SUM(H2_Error) AS total_H2_errors
FROM (
    SELECT *
    FROM (
        SELECT *
            , ROW_NUMBER() OVER(PARTITION BY box_perm, H1_Error ORDER BY Rand_Str DESC) AS RANK_N
        FROM VIEW_5)
    WHERE RANK_N<=min_customer_count)
GROUP BY 1,2,3
ORDER BY 1,2,3
)

, VIEW_7 AS (
    SELECT mapped_dc,
           H1_Error,
           SUM(customer_count) AS customer_count,
           SUM(total_H2_errors) AS total_H2_errors,
           SUM(total_H2_errors) / SUM(customer_count) AS propensity_to_report_H2_error
    FROM VIEW_6
    GROUP BY 1,2
    ORDER BY 1,2
)

SELECT * FROM VIEW_7 
