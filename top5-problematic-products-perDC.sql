--- Top 5 Problematic Products per DC

WITH VIEW_1 AS (
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
                    ELSE dc END) AS dc_name
  , sku_clean_name
  , hellofresh_week_where_error_happened
  , COUNT(DISTINCT(complaint_item_id)) AS Errors
  , SUM(compensation_amount_eur) AS Total_Comp
  , CASE WHEN mapped_accountable_team = 'Production' THEN SUM(compensation_amount_eur)
      ELSE 0 END AS Production
  , CASE WHEN mapped_accountable_team = 'Procurement' THEN SUM(compensation_amount_eur)
           ELSE 0 END AS Procurement
FROM materialized_views.cc_errors_processed
WHERE hellofresh_year = '2023'
      AND compensation_amount_eur > 0 AND compensation_amount_eur < 184
      AND compound_metric_number <= 5
      AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
      AND fk_customer <> '-1'
      AND sku_clean_name IS NOT NULL
      --AND mapped_accountable_team IN ('Production','Procurement')
GROUP BY 1,2,3,mapped_accountable_team,dc
--ORDER BY 1,2
    )

, VIEW_2 AS (
  SELECT dc_name
    , sku_clean_name
    , COUNT(DISTINCT(hellofresh_week_where_error_happened)) AS Total_Weeks
    , COUNT(DISTINCT(CASE WHEN Errors>50 THEN hellofresh_week_where_error_happened ELSE NULL END)) AS High_Error_Weeks
    , SUM(Errors) AS All_Errors
    , SUM(Total_Comp) AS Total_Comp
    , SUM(Production) AS Production
    , SUM(Procurement) AS Procurement
    , ROW_NUMBER() OVER (PARTITION BY dc_name ORDER BY SUM(Production) DESC) AS Rank
    --, ROW_NUMBER() OVER (PARTITION BY dc_name ORDER BY SUM(Total_Comp) DESC) AS Rank
  FROM VIEW_1
  --WHERE
  GROUP BY 1,2
  HAVING  High_Error_Weeks>=15 /* (dc_name IN ('Banbury','Bjuv','Nuneaton','Prismalaan','Sydney','Verden') AND High_Error_Weeks>=21) OR
      (dc_name = 'Auckland' AND High_Error_Weeks>=3) OR
      (dc_name = 'Barleben' AND High_Error_Weeks>=9) OR
      (dc_name = 'Dublin' AND High_Error_Weeks>=0) OR
      (dc_name = 'Koelliken' AND High_Error_Weeks>=0) OR
      (dc_name = 'Lisses' AND High_Error_Weeks>=17) OR
      (dc_name = 'Madrid' AND High_Error_Weeks>=1) OR
      (dc_name = 'Melbourne' AND High_Error_Weeks>=15) OR
      (dc_name = 'Milan' AND High_Error_Weeks>=1) OR
      (dc_name = 'Oslo' AND High_Error_Weeks>=6) OR
      (dc_name = 'Perth' AND High_Error_Weeks>=2) */
  ORDER BY 5 DESC)




SELECT dc_name
     , sku_clean_name
     , Total_Weeks
     , High_Error_Weeks
     , All_Errors
     , Total_Comp
     , Production
     , Procurement
FROM VIEW_2
WHERE Rank<=6
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY dc_name ASC, Total_Comp DESC
