CREATE OR REPLACE PROCEDURE bronze.load_bronze()
language plpgsql
as $$

BEGIN

    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE TABLE bronze.crm_prd_info;
    TRUNCATE TABLE bronze.crm_sales_details;
    TRUNCATE TABLE bronze.erp_cust_az12;
    TRUNCATE TABLE bronze.erp_loc_a101;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    COPY bronze.crm_cust_info
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    COPY bronze.crm_prd_info
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    COPY bronze.crm_sales_details
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    COPY bronze.erp_cust_az12
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    COPY bronze.erp_loc_a101
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

    COPY bronze.erp_px_cat_g1v2
    FROM 'C:\Users\kacpe\Desktop\SQL\Warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv
        ,HEADER true
        ,DELIMITER ','
    );

END; $$

--call bronze.load_bronze()
