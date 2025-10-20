/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Locks each table in EXCLUSIVE MODE before loading to prevent concurrent writes.
    - Uses the PostgreSQL `COPY` command to load data from CSV files to bronze tables.
    - Logs the start and end time of each table load, as well as total load duration.
    
    Note for macOS users:
    On macOS, the PostgreSQL server does not have permission to access files in the user's folder 
    (e.g., Downloads), which can cause `COPY` to fail. 
    The solution is either:
        1) Use `\copy` in psql (client-side copy), or 
        2) Move/copy the files/folders to a server-readable path, such as /tmp.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
    LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := NOW();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE 'Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    LOCK TABLE bronze.crm_cust_info IN EXCLUSIVE MODE;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
        FROM '/tmp/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    LOCK TABLE bronze.crm_prd_info IN EXCLUSIVE MODE;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
        FROM '/tmp/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_detials;
    LOCK TABLE bronze.crm_sales_detials IN EXCLUSIVE MODE;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_detials
        FROM '/tmp/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE 'Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    LOCK TABLE bronze.erp_loc_a101 IN EXCLUSIVE MODE;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
        FROM '/tmp/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    LOCK TABLE bronze.erp_cust_az12 IN EXCLUSIVE MODE;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
        FROM '/tmp/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_q1v2;
    LOCK TABLE bronze.erp_px_cat_q1v2 IN EXCLUSIVE MODE;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_q1v2
        FROM '/tmp/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER: %', SQLERRM;
    RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
    RAISE NOTICE '==========================================';
END;
$$;
