/* ============================================================================
   Script Name  : 01_create_datawarehouse.sql
   Purpose      : Create Data Warehouse database and Medallion schemas
   Author       : Akash wagh
   Created On   : 2026-02-10

   Description :
   This script creates a centralized data warehouse database along with
   Bronze, Silver, and Gold schemas following the Medallion Architecture.

   - Bronze Schema : Stores raw, ingested data from source systems
   - Silver Schema : Stores cleaned, validated, and transformed data
   - Gold Schema   : Stores business-ready, aggregated, analytics datasets

   This script is idempotent and can be safely re-run.
============================================================================ */

/* Create Data Warehouse Database */
CREATE DATABASE IF NOT EXISTS datawarehouse;

/* Switch to Data Warehouse Database */
USE datawarehouse;

/* Create Bronze Schema - Raw Layer */
CREATE SCHEMA IF NOT EXISTS bronze;

/* Create Silver Schema - Cleansed & Transformed Layer */
CREATE SCHEMA IF NOT EXISTS silver;

/* Create Gold Schema - Business & Analytics Layer */
CREATE SCHEMA IF NOT EXISTS gold;
