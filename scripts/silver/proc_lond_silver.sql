/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze → Silver)
===============================================================================
Script Purpose:
    This stored procedure is part of the ETL (Extract, Transform, Load) pipeline. 
    It performs the "Transform" and "Load" phases by cleaning, standardizing, 
    and transferring data from the 'bronze' layer to the 'silver' layer.

    Specifically, the procedure:
    - Truncates target tables in the silver schema before loading.
    - Transforms raw data from the bronze layer, including code standardization,
      text normalization, and basic data validation.
    - Loads the cleaned data into the silver schema.
    - Measures and logs the duration of each table load and the total ETL batch.

Usage:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
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
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE 'Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(cst_id,
                                     cst_key,
                                     cst_firstname,
                                     cst_lastname,
                                     cst_marital_status,
                                     cst_gndr,
                                     cst_create_date
    )
    SELECT cst_id,
           cst_key,
           TRIM(cst_firstname) AS cst_firtname,
           TRIM(cst_lastname) AS cst_lastname,
           CASE upper(trim(cst_marital_status))
               WHEN 'S' THEN 'Single'
               WHEN 'M' THEN 'Married'
               ELSE 'n/a'
           END cst_marital_status, -- Normalize marital status values to readable format
           CASE WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
                WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
           END cst_gndr, -- Normalize gender values to readable format
           cst_create_date
    FROM (
            SELECT *,
                    row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
         )t WHERE flag_last = 1; -- Select the most recent record per customer
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (prd_id,
                                     cat_id,
                                     prd_key,
                                     prd_nm,
                                     prd_cost,
                                     prd_line,
                                     prd_start_dt,
                                     prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category ID
        SUBSTRING(prd_key, 7, LENGTH(RTRIM(prd_key))) AS prd_key,  -- Extract product kry
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
            END AS prd_line,  -- Map product line codes to descriptive values
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day'
            AS DATE
        ) AS prd_end_dt  -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: silver.crm_sales_detials';
    TRUNCATE TABLE silver.crm_sales_detials;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: silver.crm_sales_detials';
    INSERT INTO silver.crm_sales_detials(
                                         sls_ord_num,
                                         sls_prd_key,
                                         sls_cust_id,
                                         sls_order_dt,
                                         sls_ship_dt,
                                         sls_due_dt,
                                         sls_sales,
                                         sls_quantity,
                                         sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END AS sls_sale,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_detials;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE 'Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(cid,
                                     bdate,
                                     gender
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))  -- Remove 'NAS' prefix if present
            ELSE cid
            END cid,
        CASE WHEN bdate > now() THEN NULL
             ELSE bdate
            END AS bdate, -- Set function birthdates to Null
        CASE
            WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
            END as gender  -- Normalize gender values and handel unknown cases
    FROM bronze.erp_cust_az12;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(cid,
                                    cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
            END AS cntry  -- Normalization and Handle missing otr blank country codes
    FROM bronze.erp_loc_a101;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE 'Truncating Table: silver.erp_px_cat_q1v2';
    TRUNCATE TABLE silver.erp_px_cat_q1v2;
    start_time := NOW();
    RAISE NOTICE 'Inserting Data Into: silver.erp_px_cat_q1v2';
    INSERT INTO silver.erp_px_cat_q1v2(id,
                                       cat,
                                       subcat,
                                       maintenance
    )
    SELECT id,
           cat,
           subcat,
           maintenance
    FROM bronze.erp_px_cat_q1v2;
    end_time := NOW();
    RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER: %', SQLERRM;
        RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;
