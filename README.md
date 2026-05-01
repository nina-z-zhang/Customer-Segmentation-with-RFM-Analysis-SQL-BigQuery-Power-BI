# Customer Segmentation with RFM Analysis | SQL (BigQuery) + Power BI

![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=flat&logo=postgresql&logoColor=white)

---

## Project Objective

For this project, I focused on customer analytics and segmentation using SQL and BI tools.
This project applies **RFM (Recency, Frequency, Monetary) analysis** to a full year of transactional sales data for a direct-to-consumer e-commerce brand. Using Google BigQuery for data engineering and Power BI for visualization, I built a customer segmentation pipeline from raw monthly tables to an interactive dashboard, which classifies 287 customers into 8 behavioural segments. 
The goal: give marketing and product teams a data-backed foundation for targeted campaigns, retention strategies, and revenue prioritization.

---

## What is RFM Analysis?

RFM is a proven customer analytics framework to score customers based on three dimensions of their purchase behaviour:

| Dimension | Question it answers | Why it matters |
|---|---|---|
| **Recency (R)** | How recently did the customer buy? | Recent buyers are more likely to respond to outreach |
| **Frequency (F)** | How often do they buy? | Frequent buyers signal loyalty and engagement |
| **Monetary (M)** | How much have they spent? | High spenders drive disproportionate revenue |

Each customer receives a score from 1–10 on each dimension (using **decile scoring**), which combine into an **RFM Total Score (max 30)**. Score thresholds then map each customer to a named segment — from *Champions* (score 28–30) down to *Lost/Inactive*.

### Why I chose RFM for this project

A mid-sized e-commerce brand typically generates thousands of transactions per year but rarely has the segmentation infrastructure to act on that data. Blanket email campaigns and identical discount offers go out to every customer on the list — a costly approach that erodes margins and ignores the different value of a repeat VIP buyer vs. a customer who last purchased a year ago.

RFM is the industry-standard starting point because it requires only three fields available in virtually any transactional dataset (customer ID, order date, order value), it is interpretable by non-technical stakeholders, and it maps directly to marketing actions.

---

## Business Questions

1. **Who are our highest-value customers**, and what share of the customer base do Champions and Loyal VIPs represent?
2. **Where is the biggest volume opportunity?** Which mid-tier segment has the most customers that could be moved up the loyalty ladder with the right campaign?
3. **How large is our churn exposure?** What percentage of customers are At Risk, Require Attention, or are already Lost/Inactive?
4. **Is there a retention priority?** Should we focus resources on reactivating dormant customers or on defending the high-value ones we already have?
5. **What does the full segmentation funnel look like?** How are customers distributed across all 8 RFM segments?

---

## Data

- **Source:** 12 monthly sales tables (`sales202501` – `sales202512`), each containing `CustomerID`, `OrderDate`, and `OrderValue`
- **Scope:** Full calendar year 2025, analysis date fixed at 2026-03-06
- **Customers:** 287 unique customers after consolidation

---

## SQL Pipeline — Step by Step

The full pipeline is in [`all_steps.sql`](./all_steps.sql) and runs in 5 steps on BigQuery.

### Step 1 — Consolidate monthly tables

```sql
CREATE OR REPLACE TABLE `rfmana.sales.sales_2025` AS
SELECT * FROM `rfmana.sales.sales202501`
UNION ALL SELECT * FROM `rfmana.sales.sales202502`
-- ... through sales202512
```

`UNION ALL` stacks all 12 monthly tables into a single unified sales table. `CREATE OR REPLACE TABLE` ensures the pipeline is fully idempotent — safe to re-run without duplicating data.

---

### Step 2 — Calculate RFM raw metrics (CTEs + Window Functions)

```sql
CREATE OR REPLACE VIEW `rfmana.sales.rfm_metrics` AS
WITH current_date AS (
  SELECT DATE('2026-03-06') AS analysis_date
),
rfm AS (
  SELECT
    CustomerID,
    MAX(OrderDate) AS last_order_date,
    DATE_DIFF((SELECT analysis_date FROM current_date), MAX(OrderDate), DAY) AS recency,
    COUNT(*) AS frequency,
    SUM(OrderValue) AS monetary
  FROM `rfmana.sales.sales_2025`
  GROUP BY CustomerID
)
SELECT
  rfm.*,
  ROW_NUMBER() OVER (ORDER BY recency ASC) AS r_rank,
  ROW_NUMBER() OVER (ORDER BY frequency DESC) AS f_rank,
  ROW_NUMBER() OVER (ORDER BY monetary DESC) AS m_rank
FROM rfm;
```

**Advanced functions used:**

- **CTEs (`WITH` clauses):** Two chained CTEs keep the logic modular — `current_date` isolates the anchor date for our analysis so it's easy to update, and `rfm` computes the raw metrics before ranking. 
- **`DATE_DIFF()`:** Calculates exact day-level recency from the fixed analysis date (can be updated throughout the year) to each customer's last order.
- **`ROW_NUMBER() OVER()`:** A window function that assigns a unique rank to every customer within the full dataset, ordered by each RFM dimension.

---

### Step 3 — Assign decile scores (NTILE)

```sql
CREATE OR REPLACE VIEW `rfmana.sales.rfm_scores` AS
SELECT *,
  NTILE(10) OVER (ORDER BY r_rank DESC) AS r_score,
  NTILE(10) OVER (ORDER BY f_rank DESC) AS f_score,
  NTILE(10) OVER (ORDER BY m_rank DESC) AS m_score
FROM `rfmana.sales.rfm_metrics`;
```

**`NTILE(10)`** is a window function that divides the ranked customer list into 10 equal deciles, assigning scores 1–10. Score 10 = top 10% of customers on that dimension. I used deciles rather than fixed thresholds to make the scoring relative and applicable to any dataset size, as it automatically adjusts when new customers are added.

---

### Step 4 — Calculate total RFM score

```sql
CREATE OR REPLACE VIEW `rfmana.sales.rfm_total_score` AS
SELECT
  CustomerID, recency, frequency, monetary,
  r_score, f_score, m_score,
  (r_score + f_score + m_score) AS rfm_total_score
FROM `rfmana.sales.rfm_scores`
ORDER BY rfm_total_score DESC;
```

Simply adding the respective scores of each dimension gives each customer a total out of 30. This view is directly needed for the final segmentation step.

---

### Step 5 — Assign business segments (CASE WHEN → final table)

```sql
CREATE OR REPLACE TABLE `rfmana.sales.rfm_segments_final` AS
SELECT *,
  CASE
    WHEN rfm_total_score >= 28 THEN 'Champions'
    WHEN rfm_total_score >= 24 THEN 'Loyal VIPs'
    WHEN rfm_total_score >= 20 THEN 'Potential Loyalists'
    WHEN rfm_total_score >= 16 THEN 'Promising'
    WHEN rfm_total_score >= 12 THEN 'Engaged'
    WHEN rfm_total_score >= 8  THEN 'Requires Attention'
    WHEN rfm_total_score >= 4  THEN 'At Risk'
    ELSE 'Lost/Inactive'
  END AS rfm_segment
FROM `rfmana.sales.rfm_total_score`
ORDER BY rfm_total_score DESC;
```

`CASE WHEN` translates the numeric score into easy to understand labels. This step is saved as a **TABLE** (not a view) because it is the final BI-ready output exported to Power BI.

---

## Dashboard

> *Power BI — RFM Analysis Dashboard*

<img width="1600" height="908" alt="dashboard_screenshot" src="https://github.com/user-attachments/assets/c2429d4d-5837-4394-8e96-0632db921ae0" />


*Data from BigQuery export (`rfm_segments_final` table). Visualizes: segment distribution bar chart, full customer detail table with RFM scores, and segment KPI summary.*

---

## Business Insights

### 1. High-value customers are a small but critical base
Champions and Loyal VIPs together account for only **63 customers (22% of the base)**, yet they represent the customers with the highest recency, frequency, and spend scores. For a DTC brand, this cohort likely drives a disproportionate share of revenue. Protecting and nurturing this segment — through early access, loyalty rewards, or dedicated account treatment — should be the highest-priority retention activity.

### 2. Engaged is the largest segment and the biggest upgrade opportunity
With **61 customers (21.3%)**, the Engaged segment is the most populated. These are customers who buy regularly enough to score well on frequency but haven't crossed the level to Promising or Loyal VIP. They are the most actionable mid-funnel group: a targeted upsell campaign, a product recommendation engine, or a loyalty programme nudge could move a meaningful share into higher-value segments.

### 3. Over a quarter of customers carry churn risk
**At Risk (38) + Requires Attention (32) + Lost/Inactive (7) = 77 customers, or 26.8% of the base.** This is a significant and addressable churn exposure. At Risk customers in particular (who have purchased before but whose recency and/or frequency scores are declining) are still reachable with a well-timed win-back offer, whereas Lost/Inactive customers may not justify the marketing spend for reactivation.

### 4. Lost/Inactive customers are minimal — but worth profiling
Only **7 customers (2.4%)** have fully lapsed. This is relatively healthy for a one-year dataset and suggests most disengaged customers fall into the *At Risk* or *Requires Attention* window, where recovery is still viable. The business should profile these 7 to check whether their lapse correlates with a specific product category, acquisition channel, or seasonal cohort.

### 5. The mid-tier is well populated — the funnel has healthy volume
The combined Promising + Potential Loyalists segment totals **86 customers (30%)**, representing a strong mid-tier pipeline. If the business can convert even 20–30% of this group upward over the next 6 months, it meaningfully grows the Champions and Loyal VIP base without needing to acquire new customers.

---

**Strategic priority summary:**
- **Defend the top (Champions + VIPs):** High-cost to replace, high-revenue impact.
- **Convert the middle (Engaged + Promising):** Highest volume, highest conversion potential.
- **Recover the at-risk (At Risk + Requires Attention):** 26.8% of base — act before they become Lost/Inactive.


