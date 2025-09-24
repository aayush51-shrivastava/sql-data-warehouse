/*
  Silver Layer Data Evaluation
  Purpose: Validate data quality in silver layer tables
  Notes:
    - Focuses on silver.crm_cust_info
    - Extend with additional checks for other silver tables
*/

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

-- Standardization check: marital_status (expected: Married, Single, Unknown)
select distinct cci.cst_marital_status
from silver.crm_cust_info cci;

-- Standardization check: gndr (expected: Male, Female, Unknown)
select distinct cci.cst_gndr
from silver.crm_cust_info cci;
