/*
  Silver Layer Data Evaluation
  Purpose: Validate data quality in silver layer tables
  Notes:
    - Extend with additional checks for other silver tables
*/

-- Evaluating silver.crm_cust_info
-- Null check: cst_id (should not exist)
select cci.cst_id,
       cci.cst_key,
       cci.cst_firstname,
       cci.cst_lastname,
       cci.cst_marital_status,
       cci.cst_gndr,
       cci.cst_create_date,
       cci.dwh_create_time
from silver.crm_cust_info cci
where cci.cst_id is null;

-- Duplicate check: cst_id (expect no duplicates)
select sub.dedup_entry,
       sub.cst_id,
       sub.cst_key,
       sub.cst_firstname,
       sub.cst_lastname,
       sub.cst_marital_status,
       sub.cst_gndr,
       sub.cst_create_date
from (select row_number() over (
    partition by cst_id
    order by cst_create_date desc nulls last
    ) as dedup_entry,
             cci.cst_id,
             cci.cst_key,
             cci.cst_firstname,
             cci.cst_lastname,
             cci.cst_marital_status,
             cci.cst_gndr,
             cci.cst_create_date
      from silver.crm_cust_info cci) sub
where dedup_entry > 1;

-- Duplicate check: cst_key (may exist, depends on source system)
select sub.dedup_number,
       sub.cst_id,
       sub.cst_key,
       sub.cst_firstname,
       sub.cst_lastname,
       sub.cst_marital_status,
       sub.cst_gndr,
       sub.cst_create_date
from (select row_number() over (
    partition by cci.cst_key
    order by cst_create_date desc nulls last
    ) as dedup_number,
             cci.cst_id,
             cci.cst_key,
             cci.cst_firstname,
             cci.cst_lastname,
             cci.cst_marital_status,
             cci.cst_gndr,
             cci.cst_create_date
      from silver.crm_cust_info cci) sub
where dedup_number > 1;

-- Standardization check: marital_status (expected: Married, Single,
-- Unknown)
select distinct cci.cst_marital_status
from silver.crm_cust_info cci;

-- Standardization check: gndr (expected: Male, Female, Unknown)
select distinct cci.cst_gndr
from silver.crm_cust_info cci;


-- Evaluating silver.crm_prd_info
-- Null check: prd_id (should not exist due to Bronze null removal)
select cpi.prd_id,
       cpi.cat_id,
       cpi.prd_key,
       cpi.prd_nm,
       cpi.prd_cost,
       cpi.prd_line,
       cpi.prd_start_dt,
       cpi.prd_end_dt,
       cpi.dwh_create_time
from silver.crm_prd_info cpi
where cpi.prd_id is null;

-- Duplicate check: prd_id (expect no duplicates)
select sub.dedup_entry,
       sub.prd_id,
       sub.cat_id,
       sub.prd_key,
       sub.prd_nm,
       sub.prd_cost,
       sub.prd_line,
       sub.prd_start_dt,
       sub.prd_end_dt
from (select row_number() over (
    partition by prd_id
    order by prd_start_dt desc nulls last
    ) as dedup_entry,
             cpi.prd_id,
             cpi.cat_id,
             cpi.prd_key,
             cpi.prd_nm,
             cpi.prd_cost,
             cpi.prd_line,
             cpi.prd_start_dt,
             cpi.prd_end_dt
      from silver.crm_prd_info cpi) sub
where dedup_entry > 1;

-- Duplicate check: prd_key (may exist, depends on source system)
select sub.dedup_number,
       sub.prd_id,
       sub.cat_id,
       sub.prd_key,
       sub.prd_nm,
       sub.prd_cost,
       sub.prd_line,
       sub.prd_start_dt,
       sub.prd_end_dt
from (select row_number() over (
    partition by prd_key
    order by prd_start_dt desc nulls last
    ) as dedup_number,
             cpi.prd_id,
             cpi.cat_id,
             cpi.prd_key,
             cpi.prd_nm,
             cpi.prd_cost,
             cpi.prd_line,
             cpi.prd_start_dt,
             cpi.prd_end_dt
      from silver.crm_prd_info cpi) sub
where dedup_number > 1;

-- Standardization check: cat_id (expected: uppercased category codes with
-- _ replacing -, e.g., CO_RF)
select distinct cpi.cat_id
from silver.crm_prd_info cpi;

-- Standardization check: prd_line (expected: Mountain, Road, Other Sales,
-- Touring, Unknown)
select distinct cpi.prd_line
from silver.crm_prd_info cpi;

-- Historization check: prd_start_dt <= prd_end_dt (should be valid)
select cpi.prd_id,
       cpi.cat_id,
       cpi.prd_key,
       cpi.prd_start_dt,
       cpi.prd_end_dt
from silver.crm_prd_info cpi
where cpi.prd_start_dt > cpi.prd_end_dt;