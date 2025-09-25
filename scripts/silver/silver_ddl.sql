/*
  Silver Layer Tables
  Warning: Drops existing silver tables
  Steps:
    1. Ensure 'sql_data_warehouse' exists
    2. Connect before running
*/

-- Drop old silver tables
drop table if exists silver.crm_cust_info;

-- CRM: Cleaned customer info
create table silver.crm_cust_info
(
    cst_id int,
    cst_key varchar(20),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(10),
    cst_gndr varchar(10),
    cst_create_date date,
    dwh_create_time timestamp default current_timestamp
);

-- CRM: Cleaned product info
create table silver.crm_prd_info
(
    prd_id int,
    prd_cat varchar(4),
    prd_key varchar(15),
    prd_nm varchar(100),
    prd_cost numeric(10, 2),
    prd_line varchar(20),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_time timestamp default current_timestamp
);