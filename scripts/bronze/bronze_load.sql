/*
  Bronze Layer Data Load
  Warning: Truncates bronze tables before loading
  Steps:
    1. Ensure 'sql_data_warehouse' and bronze tables exist
    2. Connect to 'sql_data_warehouse'
    3. Update file paths as needed
*/

-- Procedure to load bronze tables
create or replace procedure bronze.load_bronze()
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
    raise notice 'Loading Bronze Layer';
    raise notice ' ';

    -- CRM tables
    raise notice 'Loading CRM Tables';

    -- Customer info
    start_time := current_timestamp;
    raise notice 'Truncating Table: bronze.crm_cust_info';
    truncate bronze.crm_cust_info;
    raise notice 'Inserting Data Into Table: bronze.crm_cust_info';
    copy bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname,
                               cst_marital_status, cst_gndr, cst_create_date)
        from 'C:\\Program Files\\PostgreSQL\\17\\data\\project_files\\data_warehouse_project\\source_crm\\cust_info.csv'
        delimiter ',' csv header;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Product info
    start_time := current_timestamp;
    raise notice 'Truncating Table: bronze.crm_prd_info';
    truncate bronze.crm_prd_info;
    raise notice 'Inserting Data Into Table: bronze.crm_prd_info';
    copy bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line,
                              prd_start_dt, prd_end_dt)
        from 'C:\\Program Files\\PostgreSQL\\17\\data\\project_files\\data_warehouse_project\\source_crm\\prd_info.csv'
        delimiter ',' csv header;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Sales details
    start_time := current_timestamp;
    raise notice 'Truncating Table: bronze.crm_sales_details';
    truncate bronze.crm_sales_details;
    raise notice 'Inserting Data Into Table: bronze.crm_sales_details';
    copy bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id,
                                   sls_order_dt, sls_ship_dt, sls_due_dt,
                                   sls_sales, sls_quantity, sls_price)
        from 'C:\\Program Files\\PostgreSQL\\17\\data\\project_files\\data_warehouse_project\\source_crm\\sales_details.csv'
        delimiter ',' csv header;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- ERP tables
    raise notice 'Loading ERP Tables';

    -- Customer demographics
    start_time := current_timestamp;
    raise notice 'Truncating Table: bronze.erp_cust_az12';
    truncate bronze.erp_cust_az12;
    raise notice 'Inserting Data Into Table: bronze.erp_cust_az12';
    copy bronze.erp_cust_az12 (cid, bdate, gen)
        from 'C:\\Program Files\\PostgreSQL\\17\\data\\project_files\\data_warehouse_project\\source_erp\\CUST_AZ12.csv'
        delimiter ',' csv header;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Customer location
    start_time := current_timestamp;
    raise notice 'Truncating Table: bronze.erp_loc_a101';
    truncate bronze.erp_loc_a101;
    raise notice 'Inserting Data Into Table: bronze.erp_loc_a101';
    copy bronze.erp_loc_a101 (cid, cntry)
        from 'C:\\Program Files\\PostgreSQL\\17\\data\\project_files\\data_warehouse_project\\source_erp\\LOC_A101.csv'
        delimiter ',' csv header;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);
    raise notice ' ';

    -- Product categories
    start_time := current_timestamp;
    raise notice 'Truncating Table: bronze.erp_px_cat_g1v2';
    truncate bronze.erp_px_cat_g1v2;
    raise notice 'Inserting Data Into Table: bronze.erp_px_cat_g1v2';
    copy bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        from 'C:\\Program Files\\PostgreSQL\\17\\data\\project_files\\data_warehouse_project\\source_erp\\PX_CAT_G1V2.csv'
        delimiter ',' csv header;
    end_time := current_timestamp;
    raise notice 'Truncate + Load Duration: %s', round(
            extract(seconds from end_time - start_time), 2);

    batch_end_time := current_timestamp;
    raise notice ' ';
    raise notice 'Loaded Bronze Layer Successfully';
    raise notice 'Time Elapsed: %s', round(
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

-- Run bronze load
call bronze.load_bronze();
