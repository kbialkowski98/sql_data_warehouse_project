--call bronze.load_bronze()

/*
=============================================================================
DDL Script: Crete bronze tables
=============================================================================
This script creates tables in the bronze schema, dropping existing tables 
if they already exist.
Run script to load data into bronze layer tables
=============================================================================
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id                  INT
    ,cst_key                VARCHAR(50)
    ,cst_firstname          VARCHAR(50)
    ,cst_lastname           VARCHAR(50)
    ,cst_marital_status     VARCHAR(50)
    ,cst_gndr               VARCHAR(50)
    ,cst_create_date        DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id                  INT
    ,prd_key                VARCHAR(50)
    ,prd_nm                 VARCHAR(50)
    ,prd_cost               INT
    ,prd_line               VARCHAR(50)
    ,prd_start_dt           TIMESTAMP
    ,prd_end_dt             TIMESTAMP
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num             VARCHAR(50)
    ,sls_prd_key            VARCHAR(50)
    ,sls_cust_id            INT
    ,sls_order_dt           INT
    ,sls_ship_dt            INT
    ,sls_due_dt             INT
    ,sls_sales              INT
    ,sls_quantity           INT
    ,sls_price              INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid                 VARCHAR(50)
    ,cntry              VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid                 VARCHAR(50)
    ,bdate              DATE
    ,gen                VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id                  VARCHAR(50)
    ,cat                VARCHAR(50)
    ,subcat             VARCHAR(50)
    ,maintenance        VARCHAR(50)
)


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
language plpgsql
as $$

DECLARE
    v_start_time TIMESTAMP;
    v_end_time  TIMESTAMP;

BEGIN

    v_start_time := clock_timestamp(); --start
    RAISE INFO '===========================================';
    RAISE INFO 'Loading BRONZE layer';
    RAISE INFO '===========================================';
    RAISE INFO '-------------------------------------------';
    RAISE INFO '## CRM ##';
    RAISE INFO '-------------------------------------------';

    RAISE INFO '>> Inserting data into: crm_cust_info';    
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    RAISE INFO '>> Inserting data into: crm_prd_info';    
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    RAISE INFO '>> Inserting data into: crm_sales_details';    
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );


    RAISE INFO '-------------------------------------------';
    RAISE INFO '## ERP ##';
    RAISE INFO '-------------------------------------------';

    RAISE INFO '>> Inserting data into: erp_cust_az12';    
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    RAISE INFO '>> Inserting data into: erp_loc_a101';    
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    RAISE INFO '>> Inserting data into: erp_px_cat_g1v2';    
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    v_end_time := clock_timestamp(); --end
    RAISE INFO '-------------------------------------------';
    RAISE INFO '>> Load duration: %', (v_end_time - v_start_time);
    RAISE INFO '-------------------------------------------';

END; $$
