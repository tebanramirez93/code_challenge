CREATE OR REPLACE TABLE `datatest-305123.code_challenge.2021_summarize`  AS

SELECT department, job, SUM(Q1) Q1, SUM(Q2) Q2, SUM(Q3) Q3, SUM(Q4) Q4



FROM 
(SELECT 
a.id, 
a.name,
CAST(SUBSTR((CASE WHEN datetime = '0' THEN NULL ELSE DATETIME END),1,10) AS DATE) datetime,
CASE WHEN CONCAT("Q",EXTRACT(QUARTER FROM (CAST(SUBSTR((CASE WHEN datetime = '0' THEN NULL ELSE DATETIME END),1,10) AS DATE)))) = "Q1" THEN 1 else 0 END as Q1,
CASE WHEN CONCAT("Q",EXTRACT(QUARTER FROM (CAST(SUBSTR((CASE WHEN datetime = '0' THEN NULL ELSE DATETIME END),1,10) AS DATE)))) = "Q2" THEN 1 else 0 END as Q2,
CASE WHEN CONCAT("Q",EXTRACT(QUARTER FROM (CAST(SUBSTR((CASE WHEN datetime = '0' THEN NULL ELSE DATETIME END),1,10) AS DATE)))) = "Q3" THEN 1 else 0 END as Q3,
CASE WHEN CONCAT("Q",EXTRACT(QUARTER FROM (CAST(SUBSTR((CASE WHEN datetime = '0' THEN NULL ELSE DATETIME END),1,10) AS DATE)))) = "Q4" THEN 1 else 0 END as Q4,

a.department_id,
b.department,
c.job

FROM 

(SELECT * FROM `datatest-305123.code_challenge.hired_employees_refined` ) as a 

LEFT JOIN 
`datatest-305123.code_challenge.departments_refined`   as b 

ON a.department_id = b.id

LEFT JOIN 
`datatest-305123.code_challenge.jobs_refined` as c 

ON a.job_id = c.id) WHERE EXTRACT(YEAR FROM datetime) = 2021 AND department is not null

GROUP BY 1,2
ORDER BY 1,2