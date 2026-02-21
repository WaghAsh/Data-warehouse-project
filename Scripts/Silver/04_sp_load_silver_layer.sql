/* ============================================================================
   Script Name  : 04_sp_load_silver_layer.sql
   Purpose      : Load Data from Bronze Layer into Silver Layer
   Author       : Data Engineering Team
   Created On   : 2026-02-21

   Description:
   This stored procedure loads cleaned and transformed data from
   Bronze schema tables into Silver schema tables.

   Key Features:
   - Section-wise execution logging
   - Step-level duration tracking
   - Full load duration tracking
   - Error handling with transaction rollback
   - TRUNCATE before INSERT to prevent duplicate data
   - Idempotent and safe for re-execution

   Load Strategy:
   - Full refresh (TRUNCATE + INSERT)
   - Transformation logic embedded in SELECT statements
   - Business rules applied during load

   Error Handling:
   - Uses EXIT HANDLER FOR SQLEXCEPTION
   - Captures MySQL error message
   - Performs ROLLBACK
   - Returns structured error output
============================================================================ */

USE datawarehouse;

DELIMITER $$

DROP PROCEDURE IF EXISTS silver.sp_load_silver_layer $$

CREATE PROCEDURE silver.sp_load_silver_layer()
BEGIN

    /* =====================================================
       1️⃣ Variable Declaration
    ===================================================== */
    DECLARE v_start_full DATETIME;
    DECLARE v_end_full DATETIME;

    DECLARE v_step_start DATETIME;
    DECLARE v_step_end DATETIME;

    DECLARE v_error_msg TEXT;


    /* =====================================================
       2️⃣ Error Handling Block
       - Captures SQL errors
       - Rolls back transaction
       - Returns error details
    ===================================================== */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error_msg = MESSAGE_TEXT;

        ROLLBACK;

        SELECT '❌ ERROR OCCURRED DURING SILVER LOAD' AS Status,
               v_error_msg AS Error_Message,
               NOW() AS Error_Time;
    END;


    /* =====================================================
       3️⃣ Start Full Load Tracking
    ===================================================== */
    SET v_start_full = NOW();

    SELECT '🚀 Silver Layer Load Started' AS Message,
           v_start_full AS Start_Time;

    START TRANSACTION;



    /* =====================================================
       SECTION 1: CRM PRODUCT LOAD
       - Category extraction
       - Product line mapping
       - Cost standardization
       - End date derivation using LEAD()
    ===================================================== */
    SELECT '📦 Loading CRM Product Table...' AS Section;

    SET v_step_start = NOW();

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info(
        prd_id, prd_key, cat_id, prd_nm, prd_cost,
        prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        SUBSTRING(prd_key,7),
        REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
        prd_nm,
        IFNULL(prd_cost,0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(
            DATE_SUB(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
                INTERVAL 1 DAY
            ) AS DATE
        )
    FROM bronze.crm_prd_info;

    SET v_step_end = NOW();

    SELECT '✅ CRM Product Loaded' AS Step_Status,
           TIMESTAMPDIFF(SECOND, v_step_start, v_step_end) AS Duration_Seconds;



    /* =====================================================
       SECTION 2: CRM CUSTOMER LOAD
       - Deduplication using ROW_NUMBER()
       - Data standardization (Marital Status, Gender)
       - Trimmed string values
    ===================================================== */
    SELECT '👤 Loading CRM Customer Table...' AS Section;

    SET v_step_start = NOW();

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1;

    SET v_step_end = NOW();

    SELECT '✅ CRM Customer Loaded' AS Step_Status,
           TIMESTAMPDIFF(SECOND, v_step_start, v_step_end) AS Duration_Seconds;



    /* =====================================================
       SECTION 3: CRM SALES LOAD
       - Invalid date handling
       - Sales recalculation logic
       - Price correction logic
    ===================================================== */
    SELECT '💰 Loading CRM Sales Table...' AS Section;

    SET v_step_start = NOW();

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 
             THEN NULL ELSE CAST(sls_order_dt AS DATE) END,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 
             THEN NULL ELSE CAST(sls_ship_dt AS DATE) END,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 
             THEN NULL ELSE CAST(sls_due_dt AS DATE) END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 
                 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales 
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / sls_quantity
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    SET v_step_end = NOW();

    SELECT '✅ CRM Sales Loaded' AS Step_Status,
           TIMESTAMPDIFF(SECOND, v_step_start, v_step_end) AS Duration_Seconds;



    /* =====================================================
       4️⃣ End Full Load
    ===================================================== */
    SET v_end_full = NOW();

    COMMIT;

    SELECT '🎯 Silver Layer Load Completed Successfully' AS Final_Status,
           v_start_full AS Start_Time,
           v_end_full AS End_Time,
           TIMESTAMPDIFF(SECOND, v_start_full, v_end_full) AS Total_Duration_Seconds;

END $$

DELIMITER ;
