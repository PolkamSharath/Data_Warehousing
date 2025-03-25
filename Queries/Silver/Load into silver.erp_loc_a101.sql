-- Load into silver.erp_loc_a101
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101( cid, cntry)
SELECT
REPLACE(cid, '-', '') AS cid,
CASE 
	WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END as cntry
FROM bronze.erp_loc_a101