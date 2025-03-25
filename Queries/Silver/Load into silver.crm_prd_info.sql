-- Load into silver.crm_prd_info
TRUNCATE TABLE silver.crm_prd_info;

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
