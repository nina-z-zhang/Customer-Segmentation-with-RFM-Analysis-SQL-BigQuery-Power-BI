-- Step 1: Append all monthly sales tables together

CREATE OR REPLACE TABLE `rfmana.sales.sales_2025` AS
SELECT * FROM `rfmana.sales.sales202501`
UNION ALL SELECT * FROM `rfmana.sales.sales202502`
UNION ALL SELECT * FROM `rfmana.sales.sales202503`
UNION ALL SELECT * FROM `rfmana.sales.sales202504`
UNION ALL SELECT * FROM `rfmana.sales.sales202505`
UNION ALL SELECT * FROM `rfmana.sales.sales202506`
UNION ALL SELECT * FROM `rfmana.sales.sales202507`
UNION ALL SELECT * FROM `rfmana.sales.sales202508`
UNION ALL SELECT * FROM `rfmana.sales.sales202509`
UNION ALL SELECT * FROM `rfmana.sales.sales202510`
UNION ALL SELECT * FROM `rfmana.sales.sales202511`
UNION ALL SELECT * FROM `rfmana.sales.sales202512`;

-- Step 2: calculate recency, frequency, monetary, then get r,f,m ranks
--- Combine views with CTEs
CREATE OR REPLACE VIEW `rfmana.sales.rfm_metrics`
AS
WITH current_date AS (
  SELECT DATE ('2026-03-06') AS analysis_date -- todays date defined as March 6th 2026
),-- start another CTE
rfm AS ( 
  SELECT
   CustomerID,
   MAX(OrderDate) AS last_order_date, 
   date_diff((select analysis_date FROM current_date), MAX(OrderDate), DAY) AS recency, 
   COUNT(*) AS frequency,
   SUM(OrderValue) AS monetary
  FROM `rfmana.sales.sales_2025` -- we use it from sales_2025 table
  GROUP BY CustomerID
)
SELECT 
 rfm. *,
 ROW_NUMBER() OVER(ORDER BY recency ASC) AS r_rank, -- rank by recency, starting from most recent ASC
 row_number() over(order by frequency DESC) AS f_rank, -- OVER for window functions
 row_number() over(order by monetary DESC) AS m_rank
FROM rfm;

-- Step 3: Assign deciles (10=best, 1= worst); best customer score of max 30
CREATE OR REPLACE VIEW `rfmana.sales.rfm_scores`
AS
SELECT 
 *,
 NTILE(10) OVER(ORDER BY r_rank DESC) AS r_score,
 NTILE(10) OVER(ORDER BY f_rank DESC) AS f_score,
 NTILE(10) OVER(ORDER BY m_rank DESC) AS m_score
FROM `rfmana.sales.rfm_metrics`;

-- Step 4: Total Score
CREATE OR REPLACE VIEW `rfmana.sales.rfm_total_score`
AS
SELECT 
 CustomerID,
 recency,
 frequency,
 monetary,
 r_score,
 f_score,
 m_score,
 (r_score + f_score + m_score) AS rfm_total_score
FROM `rfmana.sales.rfm_scores` -- select from previous table! 
ORDER BY rfm_total_score DESC;

-- Step 5: BI ready rfm segments table 
CREATE OR REPLACE TABLE `rfmana.sales.rfm_segments_final` -- this time table! not a view
AS
SELECT 
 CustomerID,
 recency,
 frequency,
 monetary,
 r_score,
 f_score,
 m_score,
 rfm_total_score,
 CASE
    WHEN rfm_total_score >= 28 THEN 'Champions' -- 28-30
    WHEN rfm_total_score >= 24 THEN 'Loyal VIPs' -- 24-27
    WHEN rfm_total_score >= 20 THEN 'Potential Loyalists' 
    WHEN rfm_total_score >= 16 THEN 'Promising'
    WHEN rfm_total_score >= 12 THEN 'Engaged'
    WHEN rfm_total_score >= 8 THEN 'Requires Attention'
    WHEN rfm_total_score >= 4 THEN 'At Risk'
    ELSE 'Lost/Inactive'
  END AS rfm_segment
FROM `rfmana.sales.rfm_total_score`
order by rfm_total_score DESC;


