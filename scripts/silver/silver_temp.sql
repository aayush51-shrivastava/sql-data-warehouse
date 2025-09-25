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

call silver.load_crm_cst_info();
call silver.load_crm_prd_info();