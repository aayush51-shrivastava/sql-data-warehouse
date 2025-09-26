/*
  Data Warehouse Initialization
  Warning:
    - Drops and recreates the 'sql_data_warehouse' database if possible
    - ALL existing data in the database will be lost
    - Ensure no active connections are using the target database

  Steps:
    1. Drop and recreate 'sql_data_warehouse'
    2. Connect to the new database
    3. Initialize base schema
    4. Setup Bronze Layer (DDL + Load)
    5. Setup Silver Layer (DDL + Load + Procedures)
    6. Setup Gold Layer (DDL + Aggregations)
    7. Print success message

  How to Run:
    Option 1 (from terminal):
      psql -h <host> -U <username> -d postgres -f init_datawarehouse.sql

    Option 2 (inside psql):
      \i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\init_datawarehouse.sql'
*/

-- ================================================================
-- Step 1: Drop and recreate the database
-- ================================================================
drop database if exists sql_data_warehouse;
create database sql_data_warehouse;

-- ================================================================
-- Step 2: Connect to the new database
-- ================================================================
\c sql_data_warehouse;

-- ================================================================
-- Step 3: Initialize base schema
-- ================================================================
\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\init_database.sql'

-- ================================================================
-- Step 4: Bronze Layer
--   - Create schema objects
--   - Load raw data
-- ================================================================
\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\bronze\\bronze_ddl.sql'

\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\bronze\\bronze_load.sql'
call bronze.load_bronze();

-- ================================================================
-- Step 5: Silver Layer
--   - Create schema objects
--   - Load and transform data
-- ================================================================
\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\silver\\silver_ddl.sql'

\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\silver\\silver_load.sql'

call silver.load_crm_cst_info();
call silver.load_crm_prd_info();
call silver.load_crm_sales_details();
call silver.load_erp_cust_az12();
call silver.load_erp_loc_a101();
call silver.load_erp_px_cat_g1v2();

-- ================================================================
-- Step 6: Gold Layer
--   - Create schema objects and aggregation views
-- ================================================================
\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\gold\\gold_ddl.sql'

-- ================================================================
-- Step 7: Success Message
-- ================================================================
\echo 'Data Warehouse created and initialized successfully!'
