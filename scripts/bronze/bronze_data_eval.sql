/*
  Bronze Layer Evaluation
  Purpose: Profile and validate bronze data before silver load
  Steps:
    1. Ensure bronze tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Run checks below for each bronze table
*/

-- Eval: crm_cust_info
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
             over (partition by tci.cst_id order by cst_create_date desc nulls last) as dedup_entry,
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
             over (partition by tci.cst_key order by cst_create_date desc nulls last) as dedup_number,
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

-- Eval: crm_prd_info
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

-- Eval: crm_sales_details
-- Temp table for crm_sales_details
drop table if exists tmp_crm_sales_details;
create temp table tmp_crm_sales_details
(
    row_id int generated always as identity,
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

-- Copy data from bronze.crm_sales_details
insert into tmp_crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id,
                                   sls_order_dt, sls_ship_dt, sls_due_dt,
                                   sls_sales, sls_quantity, sls_price)
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price
from bronze.crm_sales_details csd;

-- Inspect inserted data
select tsd.row_id,
       tsd.sls_ord_num,
       tsd.sls_prd_key,
       tsd.sls_cust_id,
       tsd.sls_order_dt,
       tsd.sls_ship_dt,
       tsd.sls_due_dt,
       tsd.sls_sales,
       tsd.sls_quantity,
       tsd.sls_price
from tmp_crm_sales_details tsd;

-- Trim and standardize string fields
update tmp_crm_sales_details tsd
set sls_ord_num = upper(trim(tsd.sls_ord_num)),
    sls_prd_key = upper(trim(tsd.sls_prd_key));

-- Remove rows with invalid product keys
delete
from tmp_crm_sales_details tsd
where tsd.sls_prd_key not in (select cpi.prd_key from silver.crm_prd_info cpi)
returning *;

-- Remove rows with invalid customer ids
delete
from tmp_crm_sales_details tsd
where tsd.sls_cust_id not in (select cci.cst_id from silver.crm_cust_info cci)
returning *;

-- Remove rows with null sls_ord_num
delete
from tmp_crm_sales_details tsd
where tsd.sls_ord_num is null;

-- Remove rows with null sls_prd_key
delete
from tmp_crm_sales_details tsd
where tsd.sls_prd_key is null;

-- Remove rows with null sls_cust_id
delete
from tmp_crm_sales_details tsd
where tsd.sls_cust_id is null;

-- Duplicate check on sls_ord_num + sls_prd_key
select sub.row_id,
       sub.sls_ord_num,
       sub.dedup_entry,
       sub.sls_prd_key,
       sub.sls_cust_id,
       sub.sls_order_dt,
       sub.sls_ship_dt,
       sub.sls_due_dt,
       sub.sls_sales,
       sub.sls_quantity,
       sub.sls_price
from (select tsd.row_id,
             tsd.sls_ord_num,
             row_number()
             over (partition by tsd.sls_ord_num, tsd.sls_prd_key) dedup_entry,
             tsd.sls_prd_key,
             tsd.sls_cust_id,
             tsd.sls_order_dt,
             tsd.sls_ship_dt,
             tsd.sls_due_dt,
             tsd.sls_sales,
             tsd.sls_quantity,
             tsd.sls_price
      from tmp_crm_sales_details tsd) sub
where dedup_entry > 1;

-- Fix invalid date formats
update tmp_crm_sales_details csd
set sls_order_dt = case
                       when length(csd.sls_order_dt) != 8 then null
                       when length(csd.sls_order_dt) = 8
                           then csd.sls_order_dt::date end,
    sls_ship_dt = case
                      when length(csd.sls_ship_dt) != 8 then null
                      when length(csd.sls_ship_dt) = 8
                          then csd.sls_ship_dt::date end,
    sls_due_dt = case
                     when length(csd.sls_due_dt) != 8 then null
                     when length(csd.sls_due_dt) = 8
                         then csd.sls_due_dt::date end
returning *;

-- Remove duplicate sls_ord_num + sls_prd_key keeping latest
delete
from tmp_crm_sales_details tsd
where tsd.row_id in (select sub.row_id
                     from (select tsd.row_id,
                                  row_number() over (
                                      partition by tsd.sls_ord_num, tsd.sls_prd_key
                                      order by tsd.sls_order_dt desc nulls last
                                      ) dedup_entry
                           from tmp_crm_sales_details tsd) sub
                     where sub.dedup_entry > 1)
returning *;

-- Check invalid date sequences
select distinct tcd.row_id,
                tcd.sls_order_dt,
                tcd.sls_ship_dt,
                tcd.sls_due_dt
from tmp_crm_sales_details tcd
where tcd.sls_order_dt > tcd.sls_ship_dt
   or tcd.sls_order_dt > tcd.sls_due_dt;

-- Check invalid sales values
select tsd.row_id,
       tsd.sls_sales
from tmp_crm_sales_details tsd
where tsd.sls_sales is null
   or tsd.sls_sales <= 0;

-- Check invalid quantity values
select tsd.row_id,
       tsd.sls_quantity
from tmp_crm_sales_details tsd
where tsd.sls_quantity is null
   or tsd.sls_quantity <= 0;

-- Check invalid price values
select tsd.row_id,
       tsd.sls_price
from tmp_crm_sales_details tsd
where tsd.sls_price is null
   or tsd.sls_price <= 0;

-- Fix sales, quantity, and price
with updated_values as (select tsd.row_id,
                               case
                                   when tsd.sls_sales is null or tsd.sls_sales <= 0
                                       then abs(tsd.sls_quantity * tsd.sls_price)
                                   else tsd.sls_sales end new_sales,
                               case
                                   when tsd.sls_quantity is null or
                                        tsd.sls_quantity <= 0
                                       then abs(tsd.sls_sales / nullif(tsd.sls_price, 0))
                                   else tsd.sls_quantity end new_quantity,
                               case
                                   when tsd.sls_price is null or tsd.sls_price <= 0
                                       then abs(tsd.sls_sales / nullif(tsd.sls_quantity, 0))
                                   else tsd.sls_price end new_price
                        from tmp_crm_sales_details tsd)
update tmp_crm_sales_details tsd
set sls_quantity = upv.new_quantity,
    sls_sales = upv.new_sales,
    sls_price = upv.new_price
from updated_values upv
where tsd.row_id = upv.row_id
returning *;

-- Check remaining nulls
select distinct tsd.sls_ord_num,
                tsd.sls_prd_key,
                tsd.sls_cust_id,
                tsd.sls_order_dt,
                tsd.sls_ship_dt,
                tsd.sls_due_dt,
                tsd.sls_sales,
                tsd.sls_quantity,
                tsd.sls_price
from tmp_crm_sales_details tsd
where tsd.sls_sales is null
   or tsd.sls_quantity is null
   or tsd.sls_price is null;

-- Distinct values for sls_quantity
select distinct tsd.sls_quantity
from tmp_crm_sales_details tsd;

-- Eval: erp_cust_az12
-- Inspect raw data from bronze.erp_cust_az12
select eca.cid,
       eca.bdate,
       eca.gen
from bronze.erp_cust_az12 eca;

-- Temp table for erp_cust_az12
drop table if exists tmp_erp_cust_az12;
create temp table tmp_erp_cust_az12
(
    cid varchar(50),
    bdate date,
    gen varchar(50)
);

-- Compare gender formatting with silver.crm_cust_info
select distinct cst_gndr
from silver.crm_cust_info;

-- Copy data into tmp_erp_cust_az12 with trimming/uppercasing
insert into tmp_erp_cust_az12 (cid, bdate, gen)
select upper(trim(eca.cid)),
       eca.bdate,
       trim(eca.gen)
from bronze.erp_cust_az12 eca;

-- Inspect inserted records
select tca.cid,
       tca.bdate,
       tca.gen
from tmp_erp_cust_az12 tca;

-- Check distinct prefixes in cid
select distinct substring(tca.cid from 1 for 5)
from tmp_erp_cust_az12 tca;
-- Found prefixes: NASAW and AW000

-- Check distinct prefixes in cst_key from silver.crm_cust_info
select distinct substring(cci.cst_key from 1 for 5)
from silver.crm_cust_info cci;
-- Found prefix: AW000

-- Distinct gender values in tmp_erp_cust_az12
select distinct tca.gen
from tmp_erp_cust_az12 tca;

-- Preview transformation logic: replace NAS prefix, standardize gender
select tca.cid,
       case
           when substring(tca.cid from 1 for 3) = 'NAS'
               then substring(tca.cid from 4)
           else tca.cid end cleaned_cid,
       tca.bdate,
       tca.gen,
       case
           when upper(tca.gen) = 'MALE' or upper(tca.gen) = 'M' then 'Male'
           when upper(tca.gen) = 'FEMALE' or upper(tca.gen) = 'F' then 'Female'
           else 'Unknown' end fixed_gender
from tmp_erp_cust_az12 tca;

-- Apply updates: fix NAS prefix and gender values
update tmp_erp_cust_az12 tca
set cid = case
              when substring(tca.cid from 1 for 3) = 'NAS'
                  then substring(tca.cid from 4)
              else tca.cid end,
    gen = case
              when upper(tca.gen) = 'MALE' then 'Male'
              when upper(tca.gen) = 'FEMALE' then 'Female'
              else 'Unknown' end;

-- Inspect updated table
select tca.cid,
       tca.bdate,
       tca.gen
from tmp_erp_cust_az12 tca;

-- Check for cid values not in silver.crm_cust_info
select tca.cid
from tmp_erp_cust_az12 tca
where tca.cid not in (select cci.cst_key
                      from silver.crm_cust_info cci);
-- None found. If any appear, business clarification required.
-- For now: remove such rows
delete
from tmp_erp_cust_az12 tca
where tca.cid not in (select cci.cst_key
                      from silver.crm_cust_info cci)
returning *;

-- Eval: erp_loc_a101
-- Inspect raw data from bronze.erp_loc_a101
select ela.cid,
       ela.cntry
from bronze.erp_loc_a101 ela;
-- Example: cid has dashes like 'AW-00011000'

-- Compare cid format with silver.erp_cust_az12
select eca.cid
from silver.erp_cust_az12 eca;
-- Example: cid has no dashes like 'AW00011000'

-- Temp table for erp_loc_a101
drop table if exists tmp_erp_loc_a101;
create temp table tmp_erp_loc_a101
(
    row_id int generated always as identity,
    cid varchar(50),
    cntry varchar(50)
);

-- Copy data into tmp_erp_loc_a101 with trimming, uppercasing, dash removal
insert into tmp_erp_loc_a101 (cid, cntry)
select upper(replace(trim(tla.cid), '-', '')),
       trim(tla.cntry)
from bronze.erp_loc_a101 tla;

-- Inspect inserted records
select tla.cid,
       tla.cntry
from tmp_erp_loc_a101 tla;

-- Null check: cid
delete
from tmp_erp_loc_a101 tla
where tla.cid is null
returning *;

-- Duplicate check: cid
select sub.dedup_entry,
       sub.cid,
       sub.cntry
from (select row_number() over (
    partition by tla.cid
    order by tla.cntry nulls last
    ) dedup_entry,
             tla.cid,
             tla.cntry
      from tmp_erp_loc_a101 tla) sub
where sub.dedup_entry > 1;

-- Remove duplicate cid (keeping first occurrence)
delete
from tmp_erp_loc_a101 tla
where tla.row_id in (select sub.row_id
                     from (select tla.row_id,
                                  row_number() over (
                                      partition by tla.cid
                                      order by tla.cntry nulls last
                                      ) dedup_entry
                           from tmp_erp_loc_a101 tla) sub
                     where sub.dedup_entry > 1)
returning *;

-- Standardization check: cntry values
select distinct tla.cntry
from tmp_erp_loc_a101 tla;

-- Standardize cntry values
update tmp_erp_loc_a101 tla
set cntry = case
                when upper(tla.cntry) in ('FR', 'FRANCE', 'FRA') then 'France'
                when upper(tla.cntry) in ('DE', 'DEU', 'GERMANY')
                    then 'Germany'
                when upper(tla.cntry) in ('US', 'USA', 'UNITED STATES')
                    then 'United States'
                when upper(tla.cntry) in ('AU', 'AUS', 'AUSTRALIA')
                    then 'Australia'
                when upper(tla.cntry) in ('CA', 'CAN', 'CANADA') then 'Canada'
                when upper(tla.cntry) != '' or upper(tla.cntry) is not null
                    then tla.cntry
                else 'Unknown' end
returning *;

-- Referential integrity check: cid in silver.crm_cust_info.cst_key
delete
from tmp_erp_loc_a101 tla
where tla.cid not in (select cci.cst_key
                      from silver.crm_cust_info cci)
returning *;

-- Eval: erp_px_cat_g1v2
-- Inspect raw data from bronze.erp_px_cat_g1v2
select epc.id,
       epc.cat,
       epc.subcat,
       epc.maintenance
from bronze.erp_px_cat_g1v2 epc;

-- Temp table for erp_px_cat_g1v2
drop table if exists tmp_erp_px_cat_g1v2;
create temp table tmp_erp_px_cat_g1v2
(
    row_id int generated always as identity,
    id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50)
);

-- Copy data into tmp_erp_px_cat_g1v2 with trimming and uppercasing
insert into tmp_erp_px_cat_g1v2(id, cat, subcat, maintenance)
select upper(trim(epc.id)),
       trim(epc.cat),
       trim(epc.subcat),
       trim(epc.maintenance)
from bronze.erp_px_cat_g1v2 epc;

-- Inspect inserted records
select tpc.row_id,
       tpc.id,
       tpc.cat,
       tpc.subcat,
       tpc.maintenance
from tmp_erp_px_cat_g1v2 tpc;

-- Duplicate check: id
select sub.row_id,
       sub.id,
       sub.dedup_entry,
       sub.cat,
       sub.subcat,
       sub.maintenance
from (select row_id,
             id,
             row_number() over (partition by tpc.id) dedup_entry,
             cat,
             subcat,
             maintenance
      from tmp_erp_px_cat_g1v2 tpc) sub
where sub.dedup_entry > 1;

-- Remove duplicates: id (keeping first occurrence)
delete
from tmp_erp_px_cat_g1v2 tpc
where tpc.row_id in (select sub.row_id
                     from (select row_id,
                                  id,
                                  row_number() over (
                                      partition by tpc.id
                                      ) dedup_entry
                           from tmp_erp_px_cat_g1v2 tpc) sub
                     where sub.dedup_entry > 1)
returning *;

-- Distinct values for cat
select distinct tpc.cat
from tmp_erp_px_cat_g1v2 tpc;

-- Distinct values for subcat
select distinct tpc.subcat
from tmp_erp_px_cat_g1v2 tpc;

-- Distinct values for maintenance
select distinct tpc.maintenance
from tmp_erp_px_cat_g1v2 tpc;

-- Standardize maintenance values
update tmp_erp_px_cat_g1v2 tpc
set maintenance = case left(upper(trim(tpc.maintenance)), 1)
                      when 'Y' then 'Yes'
                      when 'N' then 'No'
                      else 'Unknown' end;

-- Inspect updated records
select tpc.id,
       tpc.cat,
       tpc.subcat,
       tpc.maintenance
from tmp_erp_px_cat_g1v2 tpc;