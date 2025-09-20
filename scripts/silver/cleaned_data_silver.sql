--crm_cust_info
INSERT INTO silver.crm_cust_info (
     cst_id
    ,cst_key
    ,cst_firstname
    ,cst_lastname
    ,cst_marital_status
    ,cst_gndr
    ,cst_create_date
    )

SELECT
    cst_id
    ,cst_key
    ,TRIM(cst_firstname)
    ,TRIM(cst_lastname)
    ,CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'  --remove leading/trailing spaces from first name
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' --remove leading/trailing spaces from last name
        ELSE 'Unknown'    
     END cst_marital_status                                        --normalize marital status values to readable format
    ,CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Unknown'    
     END cst_gndr                                                  --normalize gender values to readable format
    ,cst_create_date
FROM
    (
    SELECT
        *
        ,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) r
    FROM
        bronze.crm_cust_info
    )
WHERE 
    r = 1                                                           --select most recent record per customer


--crm_prd_info
INSERT INTO silver.crm_prd_info (
    prd_id
    ,cat_id
    ,prd_key
    ,prd_nm
    ,prd_cost
    ,prd_line
    ,prd_start_dt
    ,prd_end_dt
    )

SELECT
    prd_id
    ,REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id              --creating category_id column and replacing '-' with '_' (JOIN erp_px_cat_g1v2)
    ,SUBSTRING(prd_key,7,(length(prd_key) - 6)) prd_key          --creating product_key column (JOIN crm_sales_details)
    ,prd_nm
    ,COALESCE(prd_cost,0) prd_cost                               --replacing NULLs with 0
    ,CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'Unknown'
     END prd_line                                               --map product line code to descriptive values
    ,CAST(prd_start_dt AS DATE) prd_start_dt                    --cast to DATE
    ,LEAD(CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 prd_end_dt --cast to DATE and set prd_end_dt to day before next prd_start_dt
FROM
    bronze.crm_prd_info
