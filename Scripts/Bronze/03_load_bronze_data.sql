/* ============================================================================
   Script Name  : 03_load_bronze_data.sql
   Purpose      : Stored Procedure to Load Bronze Layer Tables
   Author       : Data Engineering Team
   Created On   : 2026-02-12

   Description:
   This procedure loads all CRM and ERP raw CSV files into Bronze schema.
   It truncates tables before load to allow clean re-execution.
============================================================================ */

USE datawarehouse;

DELIMITER $$

DROP PROCEDURE IF EXISTS bronze.load_bronze_data $$

CREATE PROCEDURE bronze.load_bronze_data()
BEGIN

    /* ===============================================================
       CRM CUSTOMER INFO
    =============================================================== */

    TRUNCATE TABLE bronze.crm_cust_info;

    LOAD DATA INFILE 
    'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS;

    /* ===============================================================
       CRM PRODUCT INFO
    =============================================================== */

    TRUNCATE TABLE bronze.crm_prd_info;

    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS;


    /* ===============================================================
       CRM SALES DETAILS
    =============================================================== */

    TRUNCATE TABLE bronze.crm_sales_details;

    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS;


    /* ===============================================================
       ERP TABLES
    =============================================================== */

    TRUNCATE TABLE bronze.erp_cust_az12;

    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/cust_az12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS;


    TRUNCATE TABLE bronze.erp_loc_a101;

    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS;


    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS;

END $$

DELIMITER ;
