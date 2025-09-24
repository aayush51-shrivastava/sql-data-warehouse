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

    -- Insert into silver
    start_time := current_timestamp;
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
    raise notice 'Insert Duration: %s', round(
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

call silver.load_crm_cst_info()