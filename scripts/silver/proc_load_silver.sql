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
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY

        SET  @batch_start_time = GETDATE()
        -----------------------------------------------------
        -- Truncate And Load: CRM Customer Info
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.Crm_Cust_Info';
        TRUNCATE TABLE SILVER.CRM_CUST_INFO;

        PRINT '>> Inserting Data Into: Silver.Crm_Cust_Info';
        INSERT INTO SILVER.CRM_CUST_INFO (
	           [cst_id]
              ,[cst_key]
              ,[cst_firstname]
              ,[cst_lastname]
              ,[cst_marital_status]
              ,[cst_gndr]
              ,[cst_create_date]
        )
        SELECT 
	        cst_id,
	        cst_key,
	        TRIM(cst_firstname) AS cst_firstname,
	        TRIM(cst_firstname) AS cst_lastname,
	        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Female'
		         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Male'
		         ELSE 'N/A'
	        END AS cst_marital_status,
	        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		         ELSE 'N/A'
	        END AS cst_gndr,
	        cst_create_date
        FROM (
	        SELECT *,
		           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	        FROM BRONZE.CRM_CUST_INFO
	        WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -----------------------------------------------------
        -- Truncate And Load: CRM Product Info
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.Crm_Prd_Info';
        TRUNCATE TABLE SILVER.CRM_PRD_INFO;

        PRINT '>> Inserting Data Into: Silver.Crm_Prd_Info';
        INSERT INTO SILVER.CRM_PRD_INFO (
               [prd_id]
              ,[cat_id]
              ,[prd_key]
              ,[prd_nm]
              ,[prd_cost]
              ,[prd_line]
              ,[prd_start_dt]
              ,[prd_end_dt]
        )
        SELECT 
               prd_id,
               REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
               SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
               prd_nm,
               ISNULL(prd_cost,0) AS prd_cost,
               CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'N/A'
               END AS prd_line,
               CAST(prd_start_dt AS DATE) AS prd_start_dt,
               CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_date
        FROM DATAWAREHOUSE.BRONZE.CRM_PRD_INFO;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -----------------------------------------------------
        -- Truncate And Load: CRM Sales Details
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.Crm_Sales_Details';
        TRUNCATE TABLE SILVER.CRM_SALES_DETAILS;

        PRINT '>> Inserting Data Into: Silver.Crm_Sales_Details';
        INSERT INTO SILVER.CRM_SALES_DETAILS (
               [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,[sls_order_dt]
              ,[sls_ship_dt]
              ,[sls_due_dt]
              ,[sls_sales]
              ,[sls_quantity]
              ,[sls_price]
        )
        SELECT 
               sls_ord_num,
               sls_prd_key,
               sls_cust_id,
               CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
               END AS sls_order_dt,
               CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
                    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
               END AS sls_ship_dt,
               CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
                    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
               END AS sls_due_dt,
               CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity*ABS(sls_price) 
                    THEN sls_quantity*ABS(sls_price)
                    ELSE sls_sales
               END AS sls_sales,
               sls_quantity,
               CASE WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN sls_sales/NULLIF(sls_quantity,0)
                    ELSE sls_price
               END AS sls_price
        FROM DATAWAREHOUSE.BRONZE.CRM_SALES_DETAILS;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -----------------------------------------------------
        -- Truncate And Load: ERP Customer
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.Erp_Cust_Az12';
        TRUNCATE TABLE SILVER.ERP_CUST_AZ12;

        PRINT '>> Inserting Data Into: Silver.Erp_Cust_Az12';
        INSERT INTO SILVER.ERP_CUST_AZ12 (
               cid,
               bdate,
               gen
        )
        SELECT  
               CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                    ELSE cid
               END AS cid, 
               CASE WHEN bdate > GETDATE() THEN NULL
                    ELSE bdate
               END AS bdate,
               CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                    ELSE 'N/A'
               END AS gen 
        FROM DATAWAREHOUSE.BRONZE.ERP_CUST_AZ12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -----------------------------------------------------
        -- Truncate And Load: ERP Location
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.Erp_Loc_A101';
        TRUNCATE TABLE SILVER.ERP_LOC_A101;

        PRINT '>> Inserting Data Into: Silver.Erp_Loc_A101';
        INSERT INTO SILVER.ERP_LOC_A101 (
               cid,
               cntry
        )
        SELECT 
	        REPLACE(cid,'-','') AS cid,
	        CASE WHEN TRIM(cntry) IN ('USA','US')  THEN 'United States'
		         WHEN TRIM(cntry) = 'DE' THEN 'Germany'	
		         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		         ELSE TRIM(cntry)
	        END AS cntry
        FROM BRONZE.ERP_LOC_A101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -----------------------------------------------------
        -- Truncate And Load: ERP Product Category
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.Erp_Px_Cat_G1V2';
        TRUNCATE TABLE SILVER.ERP_PX_CAT_G1V2;

        PRINT '>> Inserting Data Into: Silver.Erp_Px_Cat_G1V2';
        INSERT INTO SILVER.ERP_PX_CAT_G1V2 (
               id,
               cat,
               subcat,
               maintenance
        ) 
        SELECT 
               id,
               cat,
               subcat,
               maintenance
        FROM BRONZE.ERP_PX_CAT_G1V2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

        SET  @batch_end_time = GETDATE()
        PRINT '============================================'
        PRINT 'Silver Layer Load Completed Successfully'
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' Seconds'
        PRINT '============================================'

    END TRY

    BEGIN CATCH
        PRINT '============================================'
        PRINT 'Error Occurred During Loading Silver Layer'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================'
    END CATCH
END

EXEC silver.load_silver;
