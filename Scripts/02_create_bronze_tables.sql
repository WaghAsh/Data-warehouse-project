/* ============================================================================
   Script Name  : 02_create_bronze_tables.sql
   Purpose      : Create Bronze Layer Tables (Raw Data Layer)
   Author       : Data Engineering Team
   Created On   : 2026-02-12

   Description:
   This script creates all CRM and ERP tables inside the Bronze schema.
   Bronze layer stores raw data exactly as received from source systems.
   Tables are dropped before creation to allow clean re-execution.

   Note:
   - No transformations are applied in Bronze layer.
   - Structure matches source system.
============================================================================ */

USE datawarehouse;

-- ============================================================================
-- CRM TABLES
-- ============================================================================

-- Drop and recreate CRM Customer Information table
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
    cst_id bigint,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(20),
    cst_gndr varchar(20),
    cst_create_date date
);

-- Drop and recreate CRM Product Information table
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt datetime,
    prd_end_dt datetime
);


-- Drop and recreate CRM Sales Details table
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

-- ============================================================================
-- ERP TABLES
-- ============================================================================

-- Drop and recreate ERP Customer Additional Info table
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
    CID varchar(50),
    BDATE date,
    GEN varchar(50)
);

-- Drop and recreate ERP Location table
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
    CID varchar(50),
    CNTRY varchar(50)
);

-- Drop and recreate ERP Product Category table
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
    ID varchar(50),
    CAT varchar(50),
    SUBCAT varchar (50),
    MAINTENANCE varchar (50)
);

-- ============================================================================
-- End of Script
-- ============================================================================
