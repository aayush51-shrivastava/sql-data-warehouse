/*
  Data Warehouse Data Refresh Script
  Purpose: Refreshes data in Bronze, Silver, and Gold layers
  Notes:
    - Skips database creation and Bronze DDL
    - Only runs load procedures and Gold DDL
    - Ensure source CSVs are updated before running
  Steps:
    1. Connect to 'sql_data_warehouse'
    2. Load Bronze layer
    3. Load Silver layer
    4. Rebuild Gold layer
*/

-- Connect to the target database
\c sql_data_warehouse;

-- Load Bronze Layer
call bronze.load_bronze();

-- Load Silver Layer
call silver.load_crm_cst_info();
call silver.load_crm_prd_info();
call silver.load_crm_sales_details();
call silver.load_erp_cust_az12();
call silver.load_erp_loc_a101();
call silver.load_erp_px_cat_g1v2();

-- Build/Refresh Gold Layer
\i 'C:\\Users\\Administrator\\Desktop\\Projects\\SQLDataWarehouseProject\\sql-data-warehouse\\scripts\\gold\\gold_ddl.sql'

\echo 'Data Refreshed Successfully!'