/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_Customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_Customers;
GO

CREATE VIEW gold.dim_Customers AS
SELECT 
  ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS Customer_key,
  ci.cst_id AS Customer_ID,
  ci.cst_key AS Customer_Number,
  ci.cst_firstname AS First_name,
  ci.cst_lastname AS Last_name,
  cl.cntry AS Country,
  ci.cst_maritalstatus AS Marital_Status,
  CASE WHEN ci.cst_gender!='n/a' THEN ci.cst_gender	
  	 ELSE coalesce(cr.gen,'n/a')
  	 END AS Gender,
  cr.bdate AS Birthdate,
  ci.cst_create_date AS Create_date
FROM silver.crm_cust_info ci left join 
SILVER.erp_cust_az12 cr ON
ci.cst_key=cr.cid LEFT JOIN 
silver.erp_loc_a101 cl ON ci.cst_key=cl.cid;

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_Products', 'V') IS NOT NULL
    DROP VIEW gold.dim_Products;
GO

CREATE VIEW gold.dim_Products AS
SELECT
ROW_NUMBER() OVER(ORDER BY p.prd_start_date,p.prd_key) AS Product_key,
p.prd_id AS Product_id,
p.prd_key AS Product_number,
p.prd_name AS Product_name,
p.cat_id AS Category_id,
pc.cat AS Category,
pc.subcat AS Subcategory,
pc.maintenance AS Maintenance,
p.prd_cost as Product_cost,
p.prd_line as Product_line,
p.prd_start_date as Start_date
FROM SILVER.crm_prd_info p JOIN SILVER.px_cat_g1v2 pc ON 
p.cat_id=pc.id WHERE p.prd_end_date IS NULL;

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
  s.sls_ord_num AS order_number,
  s.sls_prd_key AS product_key,
  c.Customer_id AS customer_key,
  s.sls_order_dt AS order_date,
  s.sls_ship_dt AS shipping_date,
  s.sls_due_dt AS due_date,
  s.sls_sales AS sales_amount,
  s.sls_quantity AS quantity,
  s.sls_price AS price
  from SILVER.crm_sales_details s 
LEFT JOIN 
GOLD.dim_Products p ON s.sls_prd_key=p.Product_number
LEFT JOIN
GOLD.dim_Customers c ON s.sls_cust_id=c.Customer_key
 ;
GO
