
CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(order by cp.prd_start_dt, cp.prd_key) AS product_key,
cp.prd_id AS product_id,
cp.prd_key AS product_number,
cp.prd_nm AS product_name,
cp.cat_id AS category_id,
ep.cat AS category,
ep.subcat AS subcategory,
ep.maintenance AS maintenance,
cp.prd_cost AS cost,
cp.prd_line AS product_line,
cp.prd_start_dt AS start_date
FROM silver.crm_prd_info cp
LEFT JOIN silver.erp_px_cat_g1v2 ep
	ON cp.cat_id = ep.id
WHERE cp.prd_end_dt IS NULL