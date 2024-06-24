SELECT dc_name,
       SUM(Errors) AS Errors,
       SUM(Total_Comp) AS Total_Comp,
       SUM(Production) AS Production,
       SUM(Procurement) AS Procurement,
       SUM(Logistics) AS Logistics,
       SUM(Others) AS Others
FROM (
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
           COUNT(DISTINCT(complaint_item_id)) AS Errors,
           SUM(compensation_amount_eur) AS Total_Comp,
           CASE WHEN mapped_accountable_team = 'Production' THEN SUM(compensation_amount_eur) ELSE 0 END AS Production,
           CASE WHEN mapped_accountable_team = 'Procurement' THEN SUM(compensation_amount_eur) ELSE 0 END AS Procurement,
           CASE WHEN mapped_accountable_team = 'Logistics' THEN SUM(compensation_amount_eur) ELSE 0 END AS Logistics,
           CASE WHEN mapped_accountable_team NOT IN ('Production','Procurement','Logistics') THEN SUM(compensation_amount_eur) ELSE 0 END AS Others
    FROM materialized_views.cc_errors_processed
    WHERE hellofresh_year = '2023'
          AND compensation_amount_eur > 0 AND compensation_amount_eur < 184
          AND compound_metric_number <= 5
          AND dc IN ('NZ','GR','Barleben','BL','SK','Dublin','CH','LI','FR','Madrid','Melbourne','Milan','BV','MO','NO','Perth','Sydney','VE')
          AND fk_customer <> '-1'
    GROUP BY 1, mapped_accountable_team
) AS subquery
GROUP BY dc_name
ORDER BY dc_name
