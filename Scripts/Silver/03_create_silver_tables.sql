/* ============================================================================
   Script Name  : 03_create_silver_tables.sql
   Purpose      : Create Silver Layer Tables (Cleaned & Transformed Data Layer)
   Author       : Data Engineering Team
   Created On   : 2026-02-21

   Description:
   This script creates all CRM and ERP tables inside the Silver schema.

   The Silver layer stores cleaned, standardized, and transformed data
   derived from the Bronze (raw) layer.

   Key Characteristics:
   - Data cleansing applied
   - Data type corrections
   - Business rule transformations implemented
   - Ready for analytics and Gold layer consumption
   - Tables are dropped before creation to allow safe re-execution

   Note:
   - Surrogate metadata column (dwh_created_date) is added
     to track data warehouse load timestamp.
   - This script is idempotent (safe to re-run).
============================================================================ */

USE datawarehouse;

-- ============================================================================
-- CRM TABLES
-- ============================================================================

/* ----------------------------------------------------------------------------
   Table Name  : silver.crm_cust_info
   Purpose     : Stores cleansed customer master data from CRM system
   Description :
       - Standardized marital status and gender values
       - Trimmed names
       - Latest record retained per customer (deduplicated)
---------------------------------------------------------------------------- */

DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
    cst_id BIGINT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(20),
    cst_gndr VARCHAR(20),
    cst_create_date DATE,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



/* ----------------------------------------------------------------------------
   Table Name  : silver.crm_prd_info
   Purpose     : Stores transformed product master data from CRM system
   Description :
       - Product line mapped to business-friendly values
       - Product cost defaulted when null
       - Product start/end dates derived
       - Category extracted from product key
---------------------------------------------------------------------------- */

DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



/* ----------------------------------------------------------------------------
   Table Name  : silver.crm_sales_details
   Purpose     : Stores validated and corrected sales transaction data
   Description :
       - Invalid dates converted to NULL
       - Sales recalculated if inconsistent
       - Price corrected if invalid
       - Ensures numeric and date consistency
---------------------------------------------------------------------------- */

DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================================
-- ERP TABLES
-- ============================================================================

/* ----------------------------------------------------------------------------
   Table Name  : silver.erp_cust_az12
   Purpose     : Stores cleansed ERP customer demographic data
   Description :
       - CID standardized (prefix removed)
       - Future birthdates nullified
       - Gender standardized to Male/Female/N/A
---------------------------------------------------------------------------- */

DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



/* ----------------------------------------------------------------------------
   Table Name  : silver.erp_loc_a101
   Purpose     : Stores standardized country mapping from ERP system
   Description :
       - Country codes mapped to full names
       - Blank or null values converted to 'N/A'
       - CID cleaned (special characters removed)
---------------------------------------------------------------------------- */

DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
    CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



/* ----------------------------------------------------------------------------
   Table Name  : silver.erp_px_cat_g1v2
   Purpose     : Stores ERP product category hierarchy information
   Description :
       - Category and subcategory retained as provided
       - Maintenance classification preserved
---------------------------------------------------------------------------- */

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================================
-- End of Script
-- ============================================================================
