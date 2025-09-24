/*
  Bronze Eval Script
  Purpose: Profile and validate bronze data before silver load
  Steps:
    1. Ensure bronze.crm_cust_info is populated
    2. Connect to 'sql_data_warehouse'
    3. Run checks below (extend as needed for other bronze tables)
*/

-- Temp table for crm_cust_info
drop table if exists tmp_crm_cust_info;
create temp table tmp_crm_cust_info
(
    row_id int generated always as identity,
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date
);

-- Copy data from bronze.crm_cust_info
insert into tmp_crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname,
                               cst_marital_status, cst_gndr, cst_create_date)
select cci.cst_id,
       cci.cst_key,
       cci.cst_firstname,
       cci.cst_lastname,
       cci.cst_marital_status,
       cci.cst_gndr,
       cci.cst_create_date
from bronze.crm_cust_info cci;

-- Null check for cst_id
select *
from tmp_crm_cust_info
where cst_id is null;

-- Duplicate check on cst_id
select sub.dedup_entry,
       sub.row_id,
       sub.cst_id,
       sub.cst_key,
       sub.cst_firstname,
       sub.cst_lastname,
       sub.cst_marital_status,
       sub.cst_gndr,
       sub.cst_create_date
from (select row_number()
             over (partition by tci.cst_id order by cst_create_date desc nulls
                 last) as dedup_entry,
             tci.row_id,
             tci.cst_id,
             tci.cst_key,
             tci.cst_firstname,
             tci.cst_lastname,
             tci.cst_marital_status,
             tci.cst_gndr,
             tci.cst_create_date
      from tmp_crm_cust_info tci) sub
where dedup_entry > 1;

-- Duplicate check on cst_key
select sub.dedup_number,
       sub.row_id,
       sub.cst_id,
       sub.cst_key,
       sub.cst_firstname,
       sub.cst_lastname,
       sub.cst_marital_status,
       sub.cst_gndr,
       sub.cst_create_date
from (select row_number()
             over (partition by tci.cst_key order by cst_create_date desc nulls
                 last) as dedup_number,
             tci.row_id,
             tci.cst_id,
             tci.cst_key,
             tci.cst_firstname,
             tci.cst_lastname,
             tci.cst_marital_status,
             tci.cst_gndr,
             tci.cst_create_date
      from tmp_crm_cust_info tci) sub
where dedup_number > 1;

-- Distinct marital_status values
select distinct tci.cst_marital_status
from tmp_crm_cust_info tci;

-- Distinct gndr values
select distinct tci.cst_gndr
from tmp_crm_cust_info tci;
