--call silver.load_silver()

/*
=============================================================================
DDL Script: Load silver 
=============================================================================
This script creates tables in the silver schema, dropping existing tables 
if they already exist.
Run script to load data into silver layer tables
=============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
language plpgsql
as $$

DECLARE
    v_start_time TIMESTAMP;
    v_end_time  TIMESTAMP;

BEGIN

    v_start_time := clock_timestamp(); --start

    DROP TABLE IF EXISTS silver.crm_cust_info; 
    CREATE TABLE silver.crm_cust_info (
        cst_id                  INT
        ,cst_key                VARCHAR(50)
        ,cst_firstname          VARCHAR(50)
        ,cst_lastname           VARCHAR(50)
        ,cst_marital_status     VARCHAR(50)
        ,cst_gndr               VARCHAR(50)
        ,cst_create_date        DATE
        ,dwh_create_date		    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DROP TABLE IF EXISTS silver.crm_prd_info;
    CREATE TABLE silver.crm_prd_info (
        prd_id                  INT
        ,cat_id					VARCHAR(50)
        ,prd_key                VARCHAR(50)
        ,prd_nm                 VARCHAR(50)
        ,prd_cost               INT
        ,prd_line               VARCHAR(50)
        ,prd_start_dt           DATE
        ,prd_end_dt             DATE
        ,dwh_create_date		TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DROP TABLE IF EXISTS silver.crm_sales_details;
    CREATE TABLE silver.crm_sales_details (
        sls_ord_num             VARCHAR(50)
        ,sls_prd_key            VARCHAR(50)
        ,sls_cust_id            INT
        ,sls_order_dt           DATE
        ,sls_ship_dt            DATE
        ,sls_due_dt             DATE
        ,sls_sales              INT
        ,sls_quantity           INT
        ,sls_price              INT
        ,dwh_create_date		    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DROP TABLE IF EXISTS silver.erp_loc_a101;
    CREATE TABLE silver.erp_loc_a101(
        cid                 VARCHAR(50)
        ,cntry              VARCHAR(50)
        ,dwh_create_date	  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DROP TABLE IF EXISTS silver.erp_cust_az12;
    CREATE TABLE silver.erp_cust_az12 (
        cid                 VARCHAR(50)
        ,bdate              DATE
        ,gen                VARCHAR(50)
        ,dwh_create_date	  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
    CREATE TABLE silver.erp_px_cat_g1v2 (
        id                  VARCHAR(50)
        ,cat                VARCHAR(50)
        ,subcat             VARCHAR(50)
        ,maintenance        VARCHAR(50)
        ,dwh_create_date	  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    --crm_cust_info
    RAISE INFO '>> Inserting data into: silver.crm_cust_info';    
    TRUNCATE TABLE silver.crm_cust_info;
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
        r = 1;                                                           --select most recent record per customer


    --crm_prd_info
    RAISE INFO '>> Inserting data into: silver.crm_prd_info';    
    TRUNCATE TABLE silver.crm_prd_info;
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
        bronze.crm_prd_info;


    --crm_sales_details
    RAISE INFO '>> Inserting data into: silver.crm_sales_details';    
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num
        ,sls_prd_key
        ,sls_cust_id
        ,sls_order_dt
        ,sls_ship_dt
        ,sls_due_dt
        ,sls_sales
        ,sls_quantity
        ,sls_price
    )

    SELECT
        sls_ord_num
        ,sls_prd_key
        ,sls_cust_id
        ,CASE
            WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) <> 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END sls_order_dt                                                                   --NULL if 0 or invalid length, else cast to date
        ,CASE
            WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) <> 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END sls_ship_dt
        ,CASE                                                                               --NULL if 0 or invalid length, else cast to date
            WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) <> 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END sls_due_dt
        ,CASE                                                                               --NULL if 0 or invalid length, else cast to date
            WHEN sls_sales <= 0 or sls_sales IS NULL OR sls_sales <> ABS(sls_quantity) * ABS(sls_price) THEN sls_price * sls_quantity
            ELSE sls_sales
        END sls_sales                                                                      --Recalculate sales if orginal value is missing or incorrenct
        ,sls_quantity
        ,CASE
            WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END sls_price                                                                      --calculate price if orginal value is invalid
    FROM
        bronze.crm_sales_details;

    --erp_cust_az_12
    RAISE INFO '>> Inserting data into: silver.erp_cust_az12';   
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid
        ,bdate
        ,gen
    )

    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))      --Remove 'NAS' prefix
            ELSE cid
        END cid
        ,CASE
            WHEN bdate > CURRENT_DATE THEN NULL                         --Set to NULL if date is in the future
            ELSE bdate
        END bdate
        ,CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'      --Normalize gender values and handle unknown cases
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'Unknown'
        END gen
    FROM
        bronze.erp_cust_az12;

    --erp_loc_a101
    RAISE INFO '>> Inserting data into: silver.erp_loc_a101';   
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101(
        cid
        ,cntry
    )

    SELECT
        REPLACE(cid,'-','') cid
        ,CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
        ELSE TRIM(cntry)
        END cntry                                                       --Normalize and fix missing or black coutry codes
    FROM
        bronze.erp_loc_a101;


    --erp_px_cat_g1v2
    RAISE INFO '>> Inserting data into: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2(
        id
        ,cat
        ,subcat
        ,maintenance 
    )

    SELECT
        id
        ,cat
        ,subcat
        ,maintenance 
    FROM bronze.erp_px_cat_g1v2;

    v_end_time := clock_timestamp(); --end
    RAISE INFO '-------------------------------------------';
    RAISE INFO '>> Load duration: %', (v_end_time - v_start_time);
    RAISE INFO '-------------------------------------------';

END; $$
