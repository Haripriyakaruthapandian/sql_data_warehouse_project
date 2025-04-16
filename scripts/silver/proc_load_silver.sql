/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time=GETDATE();
	PRINT '=====================================';
	PRINT 'LOADING SILVER LAYER';
	PRINT '=====================================';
	PRINT '-------------------------------------';
	PRINT 'LOADING CRM TABLES';
	PRINT '-------------------------------------';


	
	---TABLE SILVER.CRM_CUST_INFO---
	SET @start_time=GETDATE();
	PRINT '>>TRUNCATING TABLE-> SILVER.crm_cust_info<<';
	Truncate table silver.crm_cust_info;
	PRINT '>>INSERTING INTO TABLE-> SILVER.crm_cust_info<<';
	INSERT INTO SILVER.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_maritalstatus,
									 cst_gender,cst_create_date)

	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE
		when TRIM(UPPER(cst_maritalstatus)) = 'S' THEN 'Single'
		WHEN TRIM(UPPER(cst_gender)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_maritalstatus,
	CASE
		when TRIM(UPPER(cst_gender)) = 'M' THEN 'Male'
		WHEN TRIM(UPPER(cst_gender)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gender,
	cst_create_date
	FROM
	(
	SELECT *, ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc) as flag_last
	FROM bronze.crm_cust_info WHERE cst_id is not null)t
	WHERE flag_last=1;
	
	SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION :' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';


	---TABLE SILVER.CRM_PRD_INFO---
	SET @start_time=GETDATE();
	PRINT '>>TRUNCATING TABLE-> SILVER.crm_prd_info<<';
	Truncate table silver.crm_prd_info;
	PRINT '>>INSERTING INTO TABLE-> SILVER.crm_prd_info<<';

	INSERT INTO SILVER.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_name,
	prd_cost,
	prd_line,
	prd_start_date,
	prd_end_date
	)
	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
	prd_name,
	ISNULL(prd_cost,0) as prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END prd_line,
	CAST(prd_start_date AS dATE) as prd_start_date,
	CAST(LEAD (prd_start_date) over(partition by prd_key order by prd_start_date)-1 as DATE) AS prd_end_dt
	from bronze.crm_prd_info;

	SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION :' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';

	---TABLE SILVER.CRM_SALES_DETAILS---
	SET @start_time=GETDATE();
	PRINT '>>TRUNCATING TABLE-> SILVER.crm_sales_details<<';
	Truncate table silver.crm_sales_details;
	PRINT '>>INSERTING INTO TABLE-> SILVER.crm_sales_details<<';
	INSERT INTO SILVER.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select sls_ord_num,--checked no null values
	sls_prd_key,-- checked no null values
	sls_cust_id,
	CASE WHEN sls_order_dt=0 or sls_order_dt is null or len(sls_order_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt as varchar) as Date) 
		 END
		 as sls_order_dt,
	CASE WHEN sls_ship_dt=0 or sls_ship_dt is null or len(sls_ship_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt as varchar) as Date) 
		 END
		 as sls_ship_dt,
	CASE WHEN sls_due_dt=0 or sls_due_dt is null or len(sls_due_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt as varchar) as Date) 
		 END
		 as sls_due_dt,
		 case when sls_sales is null or sls_sales<=0
		then sls_quantity*ABS(sls_price)
		else sls_sales
		end sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price<=0 
		then sls_sales / NULLIF(sls_price,0) 
	else sls_price
	end as sls_price 

	from bronze.crm_sales_details;
	SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION :' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';

	PRINT '-------------------------------------';
	PRINT 'LOADING ERP TABLES';
	PRINT '-------------------------------------';

	---TABLE BRONZE.ERP_CUST_AZ12---
	SET @start_time=GETDATE();
	PRINT '>>TRUNCATING TABLE-> SILVER.erp_cust_az12<<';

	Truncate table silver.erp_cust_az12;
		PRINT '>>INSERTING INTO TABLE-> SILVER.erp_cust_az12<<';

	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
		CASE WHEN cid like 'NAS%' then TRIM(SUBSTRING(cid,4,len(cid)))
		ELSE cid
	END as cid,
	CASE WHEN bdate>GETDATE() then NULL
		ELSE bdate
	END as bdate,
	CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
		ELSE 'n/a'
	END as gen
	FROM bronze.erp_cust_az12;
	
	SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION :' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';
	

	---TABLE SILVER.ERP_LOC_A101---
	SET @start_time=GETDATE();
	PRINT '>>TRUNCATING TABLE-> SILVER.erp_loc_a101<<';
	Truncate table silver.erp_loc_a101;
		PRINT '>>INSERTING INTO TABLE-> SILVER.erp_loc_a101<<';

	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)
	select TRIM(REPLACE(cid,'-','')) as cid,
	CASE WHEN TRIM(cntry) IN ('United States','USA','US') THEN 'United States'
		 WHEN TRIM(cntry) ='DE' THEN 'Germany'
		 WHEN TRIM(cntry) is null or cntry=' ' THEN 'n/a'
	end as cntry 
	from bronze.erp_loc_a101;
	
	SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION :' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';
	

	---TABLE SILVER.PX_CAT_G1V2---
	SET @start_time=GETDATE();

	PRINT '>>TRUNCATING TABLE-> SILVER.px_cat_g1v2<<';
	Truncate table silver.px_cat_g1v2;
	PRINT '>>INSERTING INTO TABLE-> SILVER.px_cat_g1v2<<';

	INSERT INTO silver.px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	select id,
	cat,
	subcat,
	maintenance from bronze.px_cat_g1v2;
	SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION :' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';


	SET @batch_end_time=GETDATE();

	PRINT '=======================================';
	PRINT 'LOAD EXECUTION SUCCESSFULLY COMPLETED';
	PRINT '=======================================';
	PRINT '>>TOTAL SILVER LAYER LOAD DURATION :' +CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' SECONDS';
	PRINT '>>-------------------------------------<<';

END TRY
BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='

END CATCH
END
