--- Load into silver.crm_cust_info ----
TRUNCATE TABLE silver.crm_cust_info;
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
)