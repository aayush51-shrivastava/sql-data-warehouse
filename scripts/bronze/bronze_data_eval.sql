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

-- Temp table for crm_prd_info
drop table if exists tmp_crm_prd_info;
create temp table tmp_crm_prd_info
(
    row_id int generated always as identity,
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(100),
    prd_cost numeric(10, 2),
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date
);

-- Copy data from bronze.crm_prd_info
insert into tmp_crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line,
                              prd_start_dt, prd_end_dt)
select cpi.prd_id,
       cpi.prd_key,
       cpi.prd_nm,
       cpi.prd_cost,
       cpi.prd_line,
       cpi.prd_start_dt,
       cpi.prd_end_dt
from bronze.crm_prd_info cpi;

-- Inspect inserted data
select tpi.row_id,
       tpi.prd_id,
       tpi.prd_key,
       tpi.prd_nm,
       tpi.prd_cost,
       tpi.prd_line,
       tpi.prd_start_dt,
       tpi.prd_end_dt
from tmp_crm_prd_info tpi;

-- Remove rows where both prd_id and prd_key are null
delete
from tmp_crm_prd_info tpi
where tpi.prd_id is null
  and tpi.prd_key is null;

-- Check for invalid historization (start_dt > end_dt)
select tpi.row_id,
       tpi.prd_id,
       tpi.prd_key,
       tpi.prd_start_dt,
       tpi.prd_end_dt
from tmp_crm_prd_info tpi
where tpi.prd_start_dt > tpi.prd_end_dt;

-- Correct invalid historization by swapping dates
update tmp_crm_prd_info tpi
set prd_start_dt = least(tpi.prd_start_dt, tpi.prd_end_dt),
    prd_end_dt = greatest(tpi.prd_start_dt, tpi.prd_end_dt)
where tpi.prd_start_dt > tpi.prd_end_dt
returning *;

-- Preview overlaps in product tenures
select tpi.row_id,
       tpi.prd_id,
       tpi.prd_key,
       tpi.prd_nm,
       tpi.prd_start_dt,
       (lead(tpi.prd_start_dt)
        over (partition by tpi.prd_key order by tpi.prd_start_dt) -
        interval '1 day')::date,
       tpi.prd_end_dt
from tmp_crm_prd_info tpi
order by prd_id;

-- Adjust end dates to resolve overlaps
with next_start as (select tpi.row_id,
                           (lead(tpi.prd_start_dt)
                            over (partition by tpi.prd_key order by tpi.prd_start_dt) -
                            interval '1 day')::date new_end_dt,
                           tpi.prd_end_dt
                    from tmp_crm_prd_info tpi)
update tmp_crm_prd_info tpi
set prd_end_dt = coalesce(nxt.new_end_dt, nxt.prd_end_dt)
from next_start nxt
where tpi.row_id = nxt.row_id
returning *;

-- Check remaining nulls in prd_id
select tpi.row_id,
       tpi.prd_id,
       tpi.prd_key,
       tpi.prd_nm,
       tpi.prd_cost,
       tpi.prd_line,
       tpi.prd_start_dt,
       tpi.prd_end_dt
from tmp_crm_prd_info tpi
where tpi.prd_id is null;

-- Check remaining nulls in prd_key
select tpi.row_id,
       tpi.prd_id,
       tpi.prd_key,
       tpi.prd_nm,
       tpi.prd_cost,
       tpi.prd_line,
       tpi.prd_start_dt,
       tpi.prd_end_dt
from tmp_crm_prd_info tpi
where tpi.prd_key is null;

-- Standardize prd_key and other string fields
select tpi.row_id,
       tpi.prd_id,
       upper(replace(substring(trim(tpi.prd_key) from 1 for 5), '-',
                     '_')) cat_id,
       upper(substring(trim(tpi.prd_key) from 7)) prd_key,
       upper(trim(tpi.prd_nm)) prd_nm,
       tpi.prd_cost,
       upper(trim(tpi.prd_line)) prd_line,
       tpi.prd_start_dt,
       tpi.prd_end_dt
from tmp_crm_prd_info tpi;

-- Distinct values for prd_line
select distinct tpi.prd_line
from tmp_crm_prd_info tpi;