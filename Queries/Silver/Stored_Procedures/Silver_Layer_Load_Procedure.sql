CREATE OR ALTER PROCEDURE silver.Load_Silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		set @batch_start_time = GETDATE();
		PRINT '============================================';
		PRINT '>> Loading Silver Layer';
		PRINT '============================================';
		
		PRINT '============================================';
		PRINT '>> Loading CRM Tabes';
		PRINT '============================================';
		
		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Loading Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		(
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM (
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id order by cst_create_date desc) as rn
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE rn = 1
		);
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------';
		
		-- Load into silver.crm_prd_info
		
		set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Loading Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		(SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'R' ThEN 'Road'
			WHEN 'M' ThEN 'Mountain'
			WHEN 'S' ThEN 'Other Sales'
			WHEN 'T' ThEN 'Touring'
			ELSE 'n/a'
		END as prd_line,
		CAST(prd_start_dt as DATE) as prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as DATE)  as prd_end_dt
		FROM DataWarehouse.bronze.crm_prd_info
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'Seconds';
		PRINT '>> ------------------------------'
		
		-- Load into silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Loading Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
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
		(
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) < 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE) 
		END as sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) < 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
		END as sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END as sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
			 ELSE sls_price
		END as sls_price
		FROM bronze.crm_sales_details
		)
		set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS VARCHAR) + 'Seconds';
		PRINT '>> ------------------------------'
		
		PRINT '============================================';
		PRINT '>> Loading ERP Tabes';
		PRINT '============================================';
		
		-- Load into silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Loading Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12( cid, bdate, gen)
		(SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			 ELSE cid
		END as cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			 WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			 ELSE 'n/a'
		END as gen
		FROM bronze.erp_cust_az12)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'Seconds';
		PRINT '>> ------------------------------'
		
		-- Load into silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Loading Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101( cid, cntry)
		(SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE 
			WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry
		FROM bronze.erp_loc_a101)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR) + 'Seconds';
		PRINT '>> ------------------------------'
		
		-- LOad into silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Loading Data Into: Silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		(SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR);
		PRINT '>> ------------------------------'
		
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END