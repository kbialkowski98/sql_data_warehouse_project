--call bronze.load_bronze()

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
