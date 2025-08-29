----------------------------------------------------------------------
-- Script Purpose:
-- This script creates the Data Warehouse Gold Layer dimensions (dim_customers, dim_product) 
-- and fact table (fact_sales).
-- It integrates and cleans data from the Silver Layer, applies transformations 
-- like surrogate keys, data enrichment, and handles Slowly Changing Dimensions (SCD Type 1).
----------------------------------------------------------------------


----------------------------------------------------------------------
-- View: gold.dim_customers
-- Purpose: Create customer dimension with surrogate key, enriched attributes, 
-- and gender correction (fallback from erp_cust_az12).
-- SSL / SCD Handling: Surrogate key generated with ROW_NUMBER(). 
-- Gender field handled using CASE + COALESCE (SCD1 overwrite).
----------------------------------------------------------------------
CREATE VIEW gold.dim_customers AS
SELECT
       ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
       ci.cst_id        AS customer_id,
       ci.cst_key       AS customer_number,
       ci.cst_firstname AS first_name,
       ci.cst_lastname  AS last_name,
       la.cntry         AS country,
       ci.cst_marital_status AS marital_status,
       CASE 
            WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'N/A')
       END              AS gender,
       ca.bdate         AS birthdate
FROM DataWarehouse.silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
       ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
       ON ci.cst_key = la.cid;


----------------------------------------------------------------------
-- View: gold.dim_product
-- Purpose: Create product dimension with surrogate key, enrich product with category 
-- and subcategory, filter out inactive products (prd_end_dt IS NULL).
-- SSL / SCD Handling: Surrogate key generated with ROW_NUMBER(), 
-- applying SCD1 approach by excluding closed products.
----------------------------------------------------------------------
CREATE VIEW gold.dim_product AS
SELECT 
       ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
       pn.prd_id     AS product_id,
       pn.prd_key    AS product_number,
       pn.prd_nm     AS product_name,
       pn.cat_id     AS category_id,
       pc.cat        AS category,
       pc.subcat     AS subcategory,
       pc.maintenance,
       pn.prd_cost   AS cost,
       pn.prd_line   AS product_line,
       pn.prd_start_dt AS start_date
FROM DataWarehouse.silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
       ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;


----------------------------------------------------------------------
-- View: gold.fact_sales
-- Purpose: Create fact_sales fact table linking sales orders with customer and product dimensions.
-- SSL / SCD Handling: Uses surrogate keys (customer_key, product_key) for dimensional consistency.
----------------------------------------------------------------------
CREATE VIEW gold.fact_sales AS
SELECT
      sd.sls_ord_num  AS order_number,
      pr.product_key,
      cu.customer_key,
      sd.sls_order_dt AS order_date,
      sd.sls_ship_dt  AS shipping_date,
      sd.sls_due_dt   AS due_date,
      sd.sls_sales    AS sales_amount,
      sd.sls_quantity AS quantity,
      sd.sls_price    AS price
FROM DataWarehouse.silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
       ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
       ON sd.sls_cust_id = cu.customer_id;


----------------------------------------------------------------------
-- Validation Query: Check for broken relationships between fact_sales 
-- and dimension tables (customers, products).
----------------------------------------------------------------------
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
       ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_product p
       ON p.product_key = f.product_key
WHERE c.customer_key IS NULL;
