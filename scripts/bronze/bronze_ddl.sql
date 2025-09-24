/*
  Bronze Layer Tables
  Warning: Drops existing bronze tables
  Steps:
    1. Ensure 'sql_data_warehouse' exists
    2. Connect before running
*/

-- Drop old bronze tables
drop table if exists bronze.crm_cust_info;
drop table if exists bronze.crm_prd_info;
drop table if exists bronze.crm_sales_details;
drop table if exists bronze.erp_cust_az12;
drop table if exists bronze.erp_loc_a101;
drop table if exists bronze.erp_px_cat_g1v2;

-- CRM: Customer info
create table bronze.crm_cust_info
(
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date
);

-- CRM: Product info
create table bronze.crm_prd_info
(
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(100),
    prd_cost numeric(10, 2),
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date
);

-- CRM: Sales details
create table bronze.crm_sales_details
(
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt varchar(50),
    sls_ship_dt varchar(50),
    sls_due_dt varchar(50),
    sls_sales numeric(10, 2),
    sls_quantity int,
    sls_price numeric(10, 2)
);

-- ERP: Customer demographics
create table bronze.erp_cust_az12
(
    cid varchar(50),
    bdate date,
    gen varchar(50)
);

-- ERP: Customer location
create table bronze.erp_loc_a101
(
    cid varchar(50),
    cntry varchar(50)
);

-- ERP: Product categories
create table bronze.erp_px_cat_g1v2
(
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50)
);