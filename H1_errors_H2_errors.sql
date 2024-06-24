--- to calculate the propensity to report an error in H2 if a customer reported an error in H1 vs no error reported on H1

WITH VIEW_1 AS (
SELECT DISTINCT fk_customer,
       hellofresh_week,
       CASE WHEN hellofresh_week <'2023-W27' THEN 'H1'
           WHEN hellofresh_week >= '2023-W27' THEN 'H2' END AS box_year_part,
       box_id,
       box_size,
       shift
FROM materialized_views.box_production_dimensions
WHERE hellofresh_week LIKE '2023%'
  AND fk_customer <> '-1'
  AND mapped_dc IN ('Auckland','Banbury','Barleben','Bjuv','Dublin','Koelliken','Lisses','Madrid','Melbourne','Milan','Nuneaton','Oslo','Perth','Prismalaan','Sydney','Verden')
)


, VIEW_2 AS (
SELECT fk_customer,
       --box_year_part,
       COUNT(DISTINCT CASE WHEN box_year_part = 'H1' THEN box_id END) AS H1_count,
       COUNT(DISTINCT CASE WHEN box_year_part = 'H2' THEN box_id END) AS H2_count
FROM VIEW_1
GROUP BY 1
ORDER BY 1
)

   --SELECT * FROM VIEW_2 ORDER BY 2,3 DESC/*

, VIEW_3 AS (
SELECT DISTINCT fk_customer
       , MAX(CASE WHEN hellofresh_week_where_error_happened BETWEEN '2023-W01' AND '2023-W26' THEN 1 ELSE 0 END) AS H1_Error
       , MAX(CASE WHEN hellofresh_week_where_error_happened BETWEEN '2023-W27' AND '2023-W52' THEN 1 ELSE 0 END) AS H2_Error
       --ROUND(compensation_amount_eur,2) AS compensation_amount_eur
FROM materialized_views.cc_errors_processed
WHERE hellofresh_year='2023'
AND compensation_amount_eur>0 AND compensation_amount_eur<184
AND compound_metric_number<=5
AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
AND fk_customer != '-1'
GROUP BY 1
ORDER BY 1
)

, VIEW_4 AS (
SELECT a.H1_count,
       a.H2_count,
       concat(a.H1_count,"-",a.H2_count) AS box_perm,
       COUNT(DISTINCT CASE WHEN H1_Error=1 THEN b.fk_customer END) AS H1_error_customers,
       COUNT(DISTINCT CASE WHEN H1_Error=0 THEN b.fk_customer END) AS H1_no_error_customers,
       LEAST(COUNT(DISTINCT CASE WHEN H1_Error=1 THEN b.fk_customer END), COUNT(DISTINCT CASE WHEN H1_Error=0 THEN b.fk_customer END)) AS min_customer_count
FROM VIEW_2 AS a
LEFT JOIN VIEW_3 AS b
    ON a.fk_customer = b.fk_customer
WHERE H1_count > 0 AND H2_count > 0 AND  H1_count < 27 AND H2_count < 27
GROUP BY 1,2
ORDER BY 1,2
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
SELECT box_perm
    , H1_Error
    , H1_count
    /*, CASE WHEN H1_Error = 1 AND H2_Error = 1 THEN 'H1,H2'
           WHEN H1_Error = 1 AND H2_Error = 0 THEN 'H1'
           WHEN H1_Error = 0 AND H2_Error = 1 THEN 'H2'
           WHEN H1_Error = 0 AND H2_Error = 0 THEN 'None' END AS Error_on*/
    --, H2_Error
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

SELECT --H1_count,
       H1_Error,
       SUM(customer_count),
       SUM(total_H2_errors)
FROM VIEW_6
GROUP BY 1--,2
ORDER BY 1--,2
