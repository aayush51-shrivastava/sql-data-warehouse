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

-- Evaluating silver.crm_sales_details
-- Null check: sls_ord_num (should not exist due to Bronze null removal)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price,
       csd.dwh_create_time
from silver.crm_sales_details csd
where csd.sls_ord_num is null;

-- Null check: sls_prd_key (should not exist due to Bronze null removal)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price,
       csd.dwh_create_time
from silver.crm_sales_details csd
where csd.sls_prd_key is null;

-- Null check: sls_cust_id (should not exist due to Bronze null removal)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price,
       csd.dwh_create_time
from silver.crm_sales_details csd
where csd.sls_cust_id is null;

-- Duplicate check: sls_ord_num + sls_prd_key (expect no duplicates)
select sub.dedup_entry,
       sub.sls_ord_num,
       sub.sls_prd_key,
       sub.sls_cust_id,
       sub.sls_order_dt,
       sub.sls_ship_dt,
       sub.sls_due_dt,
       sub.sls_sales,
       sub.sls_quantity,
       sub.sls_price
from (select row_number() over (
    partition by sls_ord_num, sls_prd_key
    order by sls_order_dt desc nulls last
    ) as dedup_entry,
             csd.sls_ord_num,
             csd.sls_prd_key,
             csd.sls_cust_id,
             csd.sls_order_dt,
             csd.sls_ship_dt,
             csd.sls_due_dt,
             csd.sls_sales,
             csd.sls_quantity,
             csd.sls_price
      from silver.crm_sales_details csd) sub
where dedup_entry > 1;

-- Standardization check: sls_ord_num (expected: uppercase alphanumeric
-- with no leading/trailing spaces)
select distinct csd.sls_ord_num
from silver.crm_sales_details csd;

-- Standardization check: sls_prd_key (expected: uppercase product codes, e
-- .g., "BK-R93R-62")
select distinct csd.sls_prd_key
from silver.crm_sales_details csd;

-- Date sequence check: sls_order_dt <= sls_ship_dt and sls_order_dt <=
-- sls_due_dt (should be valid)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt
from silver.crm_sales_details csd
where csd.sls_order_dt > csd.sls_ship_dt
   or csd.sls_order_dt > csd.sls_due_dt;

-- Invalid sales values check: sls_sales (should be positive)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_sales
from silver.crm_sales_details csd
where csd.sls_sales is null
   or csd.sls_sales <= 0;

-- Invalid quantity values check: sls_quantity (should be positive)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_quantity
from silver.crm_sales_details csd
where csd.sls_quantity is null
   or csd.sls_quantity <= 0;

-- Invalid price values check: sls_price (should be positive)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_price
from silver.crm_sales_details csd
where csd.sls_price is null
   or csd.sls_price <= 0;

-- Distinct values check: sls_quantity (expected: typically 1 or small
-- integers)
select distinct csd.sls_quantity
from silver.crm_sales_details csd;

-- Evaluating silver.erp_cust_az12
-- Null check: cid (should not exist due to Bronze null removal)
select eca.cid,
       eca.bdate,
       eca.gen,
       eca.dwh_create_time
from silver.erp_cust_az12 eca
where eca.cid is null;

-- Null check: bdate (should not exist due to Bronze validation)
select eca.cid,
       eca.bdate,
       eca.gen,
       eca.dwh_create_time
from silver.erp_cust_az12 eca
where eca.bdate is null;

-- Null check: gen (should not exist due to standardization)
select eca.cid,
       eca.bdate,
       eca.gen,
       eca.dwh_create_time
from silver.erp_cust_az12 eca
where eca.gen is null;

-- Duplicate check: cid (expect no duplicates)
select sub.dedup_entry,
       sub.cid,
       sub.bdate,
       sub.gen,
       sub.dwh_create_time
from (select row_number() over (
    partition by cid
    order by dwh_create_time desc nulls last
    ) as dedup_entry,
             eca.cid,
             eca.bdate,
             eca.gen,
             eca.dwh_create_time
      from silver.erp_cust_az12 eca) sub
where dedup_entry > 1;

-- Standardization check: cid (expected: 8-character alphanumeric codes starting with AW000 or similar)
select distinct eca.cid
from silver.erp_cust_az12 eca;

-- Standardization check: gen (expected: Male, Female, Unknown)
select distinct eca.gen
from silver.erp_cust_az12 eca;

-- Date validity check: bdate (should be valid and not in future)
select eca.cid,
       eca.bdate,
       eca.gen,
       eca.dwh_create_time
from silver.erp_cust_az12 eca
where eca.bdate > current_date;

-- Referential integrity check: cid in silver.crm_cust_info.cst_key
select eca.cid
from silver.erp_cust_az12 eca
where eca.cid not in (select cci.cst_key from silver.crm_cust_info cci);