/*
  Gold Layer Pre-Aggregation Evaluation
  Purpose: Validate and prepare aggregated data before creating Gold views
  Steps:
    1. Ensure Silver tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Run checks below for customer integration and deduplication
*/

-- Customer data integration across CRM and ERP
select cci.cst_id,
       cci.cst_key,
       cci.cst_firstname,
       cci.cst_lastname,
       cci.cst_marital_status,
       cci.cst_gndr,
       cci.cst_create_date,
       eca.bdate,
       eca.gen,
       ela.cntry
from silver.crm_cust_info cci
         left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid
         left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid;

-- Duplicate check: ensure joins don’t introduce multiple rows per customer
select count(*)
from (select cci.cst_id,
             cci.cst_key,
             cci.cst_firstname,
             cci.cst_lastname,
             cci.cst_marital_status,
             cci.cst_gndr,
             cci.cst_create_date,
             eca.bdate,
             eca.gen,
             ela.cntry
      from silver.crm_cust_info cci
               left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid
               left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid) sub
group by sub.cst_id
having count(*) > 1;

-- Consistency check: compare CRM gender vs ERP gender
select distinct cci.cst_gndr,
                eca.gen
from silver.crm_cust_info cci
         left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid
         left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid;

-- Rule-based resolution: prefer CRM gender unless marked as 'Unknown'
select distinct cci.cst_gndr,
                eca.gen,
                case
                    when cci.cst_gndr = 'Unknown' then eca.gen
                    else cci.cst_gndr end customer_gender
from silver.crm_cust_info cci
         left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid
         left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid;

-- Reference output: candidate query for creating gold-level customer view
select cci.cst_id customer_id,
       cci.cst_key customer_key,
       cci.cst_firstname first_name,
       cci.cst_lastname last_name,
       ela.cntry country,
       case
           when cci.cst_gndr = 'Unknown' then eca.gen
           else cci.cst_gndr end gender,
       cci.cst_marital_status marital_status,
       eca.bdate birth_date,
       cci.cst_create_date create_date
from silver.crm_cust_info cci
         left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid
         left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid;
