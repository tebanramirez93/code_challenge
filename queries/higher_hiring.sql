CREATE OR REPLACE TABLE `datatest-305123.code_challenge.higher_hiring` AS

SELECT department_id as id, department,  COUNT(*) hired
FROM 
(SELECT 
a.id, 
a.name,
CAST(SUBSTR((CASE WHEN datetime = '0' THEN NULL ELSE DATETIME END),1,10) AS DATE) datetime,
a.department_id,
b.department,
FROM 
(SELECT * FROM `datatest-305123.code_challenge.hired_employees_refined` ) as a 
LEFT JOIN 
`datatest-305123.code_challenge.departments_refined`   as b 
ON a.department_id = b.id
) WHERE EXTRACT(YEAR FROM datetime) = 2021 AND department is not null 


GROUP BY 1,2 HAVING hired > {mean}
ORDER BY 3 DESC
