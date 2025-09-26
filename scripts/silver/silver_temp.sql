/*
  Silver Layer Load Procedures
  Purpose: Clean and transform bronze data into silver tables
  Warning: Each procedure handles one silver table
  Steps:
    1. Ensure bronze tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Run procedures as needed
*/

-- Procedure to load silver.crm_cust_info
create or replace procedure silver.load_crm_cst_info()
    language plpgsql
as
$$
declare
    batch_start_time timestamp;
    batch_end_time timestamp;
    start_time timestamp;
    end_time timestamp;
begin
    batch_start_time := current_timestamp;
    raise notice 'Loading Silver Layer: crm_cust_info';
    raise notice ' ';

    -- Temp table
    start_time := current_timestamp;
    raise notice 'Creating Temp Table: tmp_crm_cust_info';
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
    end_time := current_timestamp;
    raise notice 'Temp Table Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Load from bronze
    start_time := current_timestamp;
    raise notice 'Loading data into tmp_crm_cust_info from bronze.crm_cust_info';
    insert into tmp_crm_cust_info (cst_id, cst_key, cst_firstname,
                                   cst_lastname, cst_marital_status,
                                   cst_gndr, cst_create_date)
    select cci.cst_id,
           cci.cst_key,
           cci.cst_firstname,
           cci.cst_lastname,
           cci.cst_marital_status,
           cci.cst_gndr,
           cci.cst_create_date
    from bronze.crm_cust_info cci;
    end_time := current_timestamp;
    raise notice 'Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove null cst_id
    start_time := current_timestamp;
    raise notice 'Deleting rows with null cst_id';
    delete
    from tmp_crm_cust_info
    where cst_id is null;
    end_time := current_timestamp;
    raise notice 'Null Removal Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Deduplicate cst_id
    start_time := current_timestamp;
    raise notice 'Deleting duplicate cst_id entries (keeping latest)';
    delete
    from tmp_crm_cust_info tci
    where tci.row_id not in (select sub.row_id
                             from (select row_number() over (
                                 partition by cst_id
                                 order by cst_create_date desc nulls last
                                 ) as dedup_entry,
                                          row_id
                                   from tmp_crm_cust_info) sub
                             where dedup_entry = 1);
    end_time := current_timestamp;
    raise notice 'Deduplication Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Trim string fields
    start_time := current_timestamp;
    raise notice 'Trimming string columns';
    update tmp_crm_cust_info
    set cst_key = trim(cst_key),
        cst_firstname = trim(cst_firstname),
        cst_lastname = trim(cst_lastname);
    end_time := current_timestamp;
    raise notice 'Trim Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Standardize values
    start_time := current_timestamp;
    raise notice 'Standardizing marital_status and gndr';
    update tmp_crm_cust_info
    set cst_marital_status = case upper(trim(cst_marital_status))
                                 when 'M' then 'Married'
                                 when 'S' then 'Single'
                                 else 'Unknown' end,
        cst_gndr = case upper(trim(cst_gndr))
                       when 'M' then 'Male'
                       when 'F' then 'Female'
                       else 'Unknown' end;
    end_time := current_timestamp;
    raise notice 'Standardization Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Truncate and insert into silver
    start_time := current_timestamp;
    raise notice 'Truncating Table: silver.crm_cust_info';
    truncate table silver.crm_cust_info;
    raise notice 'Inserting into silver.crm_cust_info';
    insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname,
                                      cst_lastname, cst_marital_status,
                                      cst_gndr, cst_create_date)
    select cst_id,
           cst_key,
           cst_firstname,
           cst_lastname,
           cst_marital_status,
           cst_gndr,
           cst_create_date
    from tmp_crm_cust_info;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Drop temp table
    start_time := current_timestamp;
    raise notice 'Dropping Temp Table: tmp_crm_cust_info';
    drop table tmp_crm_cust_info;
    end_time := current_timestamp;
    raise notice 'Drop Duration: %s', round(
            extract(seconds from end_time - start_time), 2);

    -- Batch complete
    batch_end_time := current_timestamp;
    raise notice ' ';
    raise notice 'Loaded Silver Layer: crm_cust_info Successfully';
    raise notice 'Total Duration: %s', round(
            extract(seconds from batch_end_time - batch_start_time), 2);
    raise notice ' ';
exception
    when others then
        raise notice ' ';
        raise warning 'Error: %', sqlerrm;
        raise notice ' ';
        raise notice 'Rolling Back Changes';
        rollback;
end;
$$;

-- Procedure to load silver.crm_prd_info
create or replace procedure silver.load_crm_prd_info()
    language plpgsql
as
$$
declare
    batch_start_time timestamp;
    batch_end_time timestamp;
    start_time timestamp;
    end_time timestamp;
begin
    batch_start_time := current_timestamp;
    raise notice 'Loading Silver Layer: crm_prd_info';
    raise notice ' ';

    -- Temp table
    start_time := current_timestamp;
    raise notice 'Creating Temp Table: tmp_crm_prd_info';
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
    end_time := current_timestamp;
    raise notice 'Temp Table Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Load from bronze
    start_time := current_timestamp;
    raise notice 'Loading data into tmp_crm_prd_info from bronze.crm_prd_info';
    insert into tmp_crm_prd_info (prd_id, prd_key, prd_nm, prd_cost,
                                  prd_line, prd_start_dt, prd_end_dt)
    select cpi.prd_id,
           cpi.prd_key,
           cpi.prd_nm,
           cpi.prd_cost,
           cpi.prd_line,
           cpi.prd_start_dt,
           cpi.prd_end_dt
    from bronze.crm_prd_info cpi;
    end_time := current_timestamp;
    raise notice 'Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove rows with both prd_id and prd_key null
    start_time := current_timestamp;
    raise notice 'Deleting rows with null prd_id and prd_key';
    delete
    from tmp_crm_prd_info tpi
    where tpi.prd_id is null
      and tpi.prd_key is null;
    end_time := current_timestamp;
    raise notice 'Null Removal Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Fix invalid historization (start_dt > end_dt)
    start_time := current_timestamp;
    raise notice 'Correcting invalid start/end dates';
    update tmp_crm_prd_info tpi
    set prd_start_dt = least(tpi.prd_start_dt, tpi.prd_end_dt),
        prd_end_dt = greatest(tpi.prd_start_dt, tpi.prd_end_dt)
    where tpi.prd_start_dt > tpi.prd_end_dt;
    end_time := current_timestamp;
    raise notice 'Historization Fix Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Adjust overlapping product tenures
    start_time := current_timestamp;
    raise notice 'Adjusting overlapping product tenures';
    with next_start as (select tpi.row_id,
                               (lead(tpi.prd_start_dt)
                                over (partition by tpi.prd_key order by tpi
                                    .prd_start_dt nulls last) -
                                interval '1 day')::date new_end_dt,
                               tpi.prd_end_dt
                        from tmp_crm_prd_info tpi)
    update tmp_crm_prd_info tpi
    set prd_end_dt = coalesce(nxt.new_end_dt, nxt.prd_end_dt)
    from next_start nxt
    where tpi.row_id = nxt.row_id;
    end_time := current_timestamp;
    raise notice 'Overlap Adjustment Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Truncate and insert into silver
    start_time := current_timestamp;
    raise notice 'Truncating Table: silver.crm_prd_info';
    truncate table silver.crm_prd_info;
    raise notice 'Inserting into silver.crm_prd_info';
    insert into silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm,
                                     prd_cost, prd_line, prd_start_dt,
                                     prd_end_dt)
    select tpi.prd_id,
           upper(replace(substring(trim(tpi.prd_key) from 1 for 5), '-', '_')),
           upper(substring(trim(tpi.prd_key) from 7)),
           upper(trim(tpi.prd_nm)),
           tpi.prd_cost,
           case upper(trim(tpi.prd_line))
               when 'M' then 'Mountain'
               when 'R' then 'Road'
               when 'S' then 'Other Sales'
               when 'T' then 'Touring'
               else 'Unknown' end,
           tpi.prd_start_dt,
           tpi.prd_end_dt
    from tmp_crm_prd_info tpi;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Drop temp table
    start_time := current_timestamp;
    raise notice 'Dropping Temp Table: tmp_crm_prd_info';
    drop table tmp_crm_prd_info;
    end_time := current_timestamp;
    raise notice 'Drop Duration: %s', round(
            extract(seconds from end_time - start_time), 2);

    -- Batch complete
    batch_end_time := current_timestamp;
    raise notice ' ';
    raise notice 'Loaded Silver Layer: crm_prd_info Successfully';
    raise notice 'Total Duration: %s', round(
            extract(seconds from batch_end_time - batch_start_time), 2);
    raise notice ' ';
exception
    when others then
        raise notice ' ';
        raise warning 'Error: %', sqlerrm;
        raise notice ' ';
        raise notice 'Rolling Back Changes';
        rollback;
end;
$$;

-- Procedure to load silver.crm_sales_details
create or replace procedure silver.load_crm_sales_details()
    language plpgsql
as
$$
declare
    batch_start_time timestamp;
    batch_end_time timestamp;
    start_time timestamp;
    end_time timestamp;
begin
    batch_start_time := current_timestamp;
    raise notice 'Loading Silver Layer: crm_sales_details';
    raise notice ' ';

    -- Temp table
    start_time := current_timestamp;
    raise notice 'Creating Temp Table: tmp_crm_sales_details';
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
    end_time := current_timestamp;
    raise notice 'Temp Table Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Load from bronze
    start_time := current_timestamp;
    raise notice 'Loading data into tmp_crm_sales_details from bronze.crm_sales_details';
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
    end_time := current_timestamp;
    raise notice 'Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Standardize string fields
    start_time := current_timestamp;
    raise notice 'Trimming and uppercasing order number and product key';
    update tmp_crm_sales_details tsd
    set sls_ord_num = upper(trim(tsd.sls_ord_num)),
        sls_prd_key = upper(trim(tsd.sls_prd_key));
    end_time := current_timestamp;
    raise notice 'Standardization Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove invalid references and nulls
    start_time := current_timestamp;
    raise notice 'Removing invalid product keys';
    delete
    from tmp_crm_sales_details tsd
    where tsd.sls_prd_key not in
          (select cpi.prd_key from silver.crm_prd_info cpi);

    raise notice 'Removing invalid customer ids';
    delete
    from tmp_crm_sales_details tsd
    where tsd.sls_cust_id not in
          (select cci.cst_id from silver.crm_cust_info cci);

    raise notice 'Removing rows with null order number, product key, or customer id';
    delete from tmp_crm_sales_details where sls_ord_num is null;
    delete from tmp_crm_sales_details where sls_prd_key is null;
    delete from tmp_crm_sales_details where sls_cust_id is null;
    end_time := current_timestamp;
    raise notice 'Reference + Null Cleanup Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Fix invalid date formats
    start_time := current_timestamp;
    raise notice 'Fixing invalid date formats';
    update tmp_crm_sales_details csd
    set sls_order_dt = case
                           when length(csd.sls_order_dt) != 8 then null
                           else csd.sls_order_dt::date end,
        sls_ship_dt = case
                          when length(csd.sls_ship_dt) != 8 then null
                          else csd.sls_ship_dt::date end,
        sls_due_dt = case
                         when length(csd.sls_due_dt) != 8 then null
                         else csd.sls_due_dt::date end;
    end_time := current_timestamp;
    raise notice 'Date Fix Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove duplicates (keeping latest)
    start_time := current_timestamp;
    raise notice 'Removing duplicate order_num + product_key entries';
    delete
    from tmp_crm_sales_details tsd
    where tsd.row_id in (select sub.row_id
                         from (select tsd.row_id,
                                      row_number() over (
                                          partition by tsd.sls_ord_num, tsd.sls_prd_key
                                          order by tsd.sls_order_dt desc nulls last
                                          ) dedup_entry
                               from tmp_crm_sales_details tsd) sub
                         where sub.dedup_entry > 1);
    end_time := current_timestamp;
    raise notice 'Deduplication Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Fix invalid sales, quantity, price
    start_time := current_timestamp;
    raise notice 'Correcting invalid sales, quantity, and price values';
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
    where tsd.row_id = upv.row_id;
    end_time := current_timestamp;
    raise notice 'Measure Fix Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Truncate and insert into silver
    start_time := current_timestamp;
    raise notice 'Truncating Table: silver.crm_sales_details';
    truncate table silver.crm_sales_details;
    raise notice 'Inserting into silver.crm_sales_details';
    insert into silver.crm_sales_details (sls_ord_num, sls_prd_key,
                                          sls_cust_id,
                                          sls_order_dt, sls_ship_dt,
                                          sls_due_dt,
                                          sls_sales, sls_quantity, sls_price)
    select tpi.sls_ord_num,
           tpi.sls_prd_key,
           tpi.sls_cust_id,
           tpi.sls_order_dt::date,
           tpi.sls_ship_dt::date,
           tpi.sls_due_dt::date,
           tpi.sls_sales,
           tpi.sls_quantity,
           tpi.sls_price
    from tmp_crm_sales_details tpi;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Drop temp table
    start_time := current_timestamp;
    raise notice 'Dropping Temp Table: tmp_crm_sales_details';
    drop table tmp_crm_sales_details;
    end_time := current_timestamp;
    raise notice 'Drop Duration: %s', round(
            extract(seconds from end_time - start_time), 2);

    -- Batch complete
    batch_end_time := current_timestamp;
    raise notice ' ';
    raise notice 'Loaded Silver Layer: crm_sales_details Successfully';
    raise notice 'Total Duration: %s', round(
            extract(seconds from batch_end_time - batch_start_time), 2);
    raise notice ' ';
exception
    when others then
        raise notice ' ';
        raise warning 'Error: %', sqlerrm;
        raise notice ' ';
        raise notice 'Rolling Back Changes';
        rollback;
end;
$$;

-- Procedure to load silver.erp_cust_az12
create or replace procedure silver.load_erp_cust_az12()
    language plpgsql
as
$$
declare
    batch_start_time timestamp;
    batch_end_time timestamp;
    start_time timestamp;
    end_time timestamp;
begin
    batch_start_time := current_timestamp;
    raise notice 'Loading Silver Layer: erp_cust_az12';
    raise notice ' ';

    -- Temp table
    start_time := current_timestamp;
    raise notice 'Creating Temp Table: tmp_erp_cust_az12';
    drop table if exists tmp_erp_cust_az12;
    create temp table tmp_erp_cust_az12
    (
        row_id int generated always as identity,
        cid varchar(50),
        bdate date,
        gen varchar(50)
    );
    end_time := current_timestamp;
    raise notice 'Temp Table Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Load from bronze
    start_time := current_timestamp;
    raise notice 'Loading data into tmp_erp_cust_az12 from bronze.erp_cust_az12';
    insert into tmp_erp_cust_az12 (cid, bdate, gen)
    select upper(trim(eca.cid)),
           eca.bdate,
           trim(eca.gen)
    from bronze.erp_cust_az12 eca;
    end_time := current_timestamp;
    raise notice 'Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Standardize cid prefix and gender
    start_time := current_timestamp;
    raise notice 'Standardizing cid prefix and gender values';
    update tmp_erp_cust_az12 tca
    set cid = case
                  when substring(tca.cid from 1 for 3) = 'NAS'
                      then substring(tca.cid from 4)
                  else tca.cid end,
        gen = case
                  when upper(tca.gen) = 'MALE' or upper(tca.gen) = 'M'
                      then 'Male'
                  when upper(tca.gen) = 'FEMALE' or upper(tca.gen) = 'F'
                      then 'Female'
                  else 'Unknown' end,
        bdate = case
                    when tca.bdate > current_date then null
                    else tca.bdate end;
    end_time := current_timestamp;
    raise notice 'Standardization Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Deduplicate cid
    start_time := current_timestamp;
    raise notice 'Deleting duplicate cid entries (keeping first occurrence)';
    delete
    from tmp_erp_cust_az12 tca
    where tca.row_id not in (select sub.row_id
                             from (select row_id,
                                          row_number() over (
                                              partition by cid
                                              order by row_id
                                              ) as dedup_entry
                                   from tmp_erp_cust_az12) sub
                             where dedup_entry = 1);
    end_time := current_timestamp;
    raise notice 'Deduplication Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove invalid customer references
    start_time := current_timestamp;
    raise notice 'Removing rows with cid not in silver.crm_cust_info';
    delete
    from tmp_erp_cust_az12 tca
    where tca.cid not in (select cci.cst_key
                          from silver.crm_cust_info cci);
    end_time := current_timestamp;
    raise notice 'Reference Cleanup Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Truncate and insert into silver
    start_time := current_timestamp;
    raise notice 'Truncating Table: silver.erp_cust_az12';
    truncate table silver.erp_cust_az12;
    raise notice 'Inserting into silver.erp_cust_az12';
    insert into silver.erp_cust_az12 (cid, bdate, gen)
    select tca.cid,
           tca.bdate,
           tca.gen
    from tmp_erp_cust_az12 tca;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Drop temp table
    start_time := current_timestamp;
    raise notice 'Dropping Temp Table: tmp_erp_cust_az12';
    drop table tmp_erp_cust_az12;
    end_time := current_timestamp;
    raise notice 'Drop Duration: %s', round(
            extract(seconds from end_time - start_time), 2);

    -- Batch complete
    batch_end_time := current_timestamp;
    raise notice ' ';
    raise notice 'Loaded Silver Layer: erp_cust_az12 Successfully';
    raise notice 'Total Duration: %s', round(
            extract(seconds from batch_end_time - batch_start_time), 2);
    raise notice ' ';
exception
    when others then
        raise notice ' ';
        raise warning 'Error: %', sqlerrm;
        raise notice ' ';
        raise notice 'Rolling Back Changes';
        rollback;
end;
$$;

-- Procedure to load silver.erp_loc_a101
create or replace procedure silver.load_erp_loc_a101()
    language plpgsql
as
$$
declare
    batch_start_time timestamp;
    batch_end_time timestamp;
    start_time timestamp;
    end_time timestamp;
begin
    batch_start_time := current_timestamp;
    raise notice 'Loading Silver Layer: erp_loc_a101';
    raise notice ' ';

    -- Temp table
    start_time := current_timestamp;
    raise notice 'Creating Temp Table: tmp_erp_loc_a101';
    drop table if exists tmp_erp_loc_a101;
    create temp table tmp_erp_loc_a101
    (
        row_id int generated always as identity,
        cid varchar(50),
        cntry varchar(50)
    );
    end_time := current_timestamp;
    raise notice 'Temp Table Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Load from bronze
    start_time := current_timestamp;
    raise notice 'Loading data into tmp_erp_loc_a101 from bronze.erp_loc_a101';
    insert into tmp_erp_loc_a101 (cid, cntry)
    select upper(replace(trim(tla.cid), '-', '')),
           trim(tla.cntry)
    from bronze.erp_loc_a101 tla;
    end_time := current_timestamp;
    raise notice 'Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove rows with null cid
    start_time := current_timestamp;
    raise notice 'Deleting rows with null cid';
    delete
    from tmp_erp_loc_a101 tla
    where tla.cid is null;
    end_time := current_timestamp;
    raise notice 'Null Removal Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Deduplicate cid
    start_time := current_timestamp;
    raise notice 'Deleting duplicate cid entries (keeping first occurrence)';
    delete
    from tmp_erp_loc_a101 tla
    where tla.row_id in (select sub.row_id
                         from (select tla.row_id,
                                      row_number() over (
                                          partition by tla.cid
                                          order by tla.cntry nulls last
                                          ) dedup_entry
                               from tmp_erp_loc_a101 tla) sub
                         where sub.dedup_entry > 1);
    end_time := current_timestamp;
    raise notice 'Deduplication Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Standardize cntry values
    start_time := current_timestamp;
    raise notice 'Standardizing cntry values';
    update tmp_erp_loc_a101 tla
    set cntry = case
                    when upper(tla.cntry) in ('FR', 'FRANCE', 'FRA')
                        then 'France'
                    when upper(tla.cntry) in ('DE', 'DEU', 'GERMANY')
                        then 'Germany'
                    when upper(tla.cntry) in ('US', 'USA', 'UNITED STATES')
                        then 'United States'
                    when upper(tla.cntry) in ('AU', 'AUS', 'AUSTRALIA')
                        then 'Australia'
                    when upper(tla.cntry) in ('CA', 'CAN', 'CANADA')
                        then 'Canada'
                    when upper(tla.cntry) != '' or upper(tla.cntry) is not null
                        then tla.cntry
                    else 'Unknown' end;
    end_time := current_timestamp;
    raise notice 'Standardization Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Remove invalid customer references
    start_time := current_timestamp;
    raise notice 'Removing rows with cid not in silver.crm_cust_info';
    delete
    from tmp_erp_loc_a101 tla
    where tla.cid not in (select cci.cst_key
                          from silver.crm_cust_info cci);
    end_time := current_timestamp;
    raise notice 'Reference Cleanup Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Truncate and insert into silver
    start_time := current_timestamp;
    raise notice 'Truncating Table: silver.erp_loc_a101';
    truncate table silver.erp_loc_a101;
    raise notice 'Inserting into silver.erp_loc_a101';
    insert into silver.erp_loc_a101 (cid, cntry)
    select tla.cid,
           tla.cntry
    from tmp_erp_loc_a101 tla;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Drop temp table
    start_time := current_timestamp;
    raise notice 'Dropping Temp Table: tmp_erp_loc_a101';
    drop table tmp_erp_loc_a101;
    end_time := current_timestamp;
    raise notice 'Drop Duration: %s', round(
            extract(seconds from end_time - start_time), 2);

    -- Batch complete
    batch_end_time := current_timestamp;
    raise notice ' ';
    raise notice 'Loaded Silver Layer: erp_loc_a101 Successfully';
    raise notice 'Total Duration: %s', round(
            extract(seconds from batch_end_time - batch_start_time), 2);
    raise notice ' ';
exception
    when others then
        raise notice ' ';
        raise warning 'Error: %', sqlerrm;
        raise notice ' ';
        raise notice 'Rolling Back Changes';
        rollback;
end;
$$;

call silver.load_crm_cst_info();
call silver.load_crm_prd_info();
call silver.load_crm_sales_details();
call silver.load_erp_cust_az12();
call silver.load_erp_loc_a101();