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
