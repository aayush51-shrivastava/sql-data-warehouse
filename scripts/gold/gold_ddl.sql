/*
  Gold Layer View Creation
  Purpose: Provide a unified customer profile by aggregating and reconciling CRM and ERP data
  Steps:
    1. Ensure Silver tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Execute to create the view
*/

/*
  Gold Layer View Creation
  Purpose: Provide a unified customer profile by aggregating and reconciling CRM and ERP data
  Steps:
    1. Ensure Silver tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Execute to create the view
*/

-- Drop existing view if it exists
drop view if exists gold.dim_customers;

-- Create customer dimension view in Gold layer
create view gold.dim_customers as
select row_number() over (order by cci.cst_id) customer_number, -- surrogate number
       cci.cst_id customer_id,
       cci.cst_key customer_key,
       cci.cst_firstname first_name,
       cci.cst_lastname last_name,
       ela.cntry country,
       case
           when cci.cst_gndr = 'Unknown' then eca.gen
           else cci.cst_gndr end gender,                        -- prefer CRM gender unless unknown
       cci.cst_marital_status marital_status,
       eca.bdate birth_date,
       cci.cst_create_date create_date
from silver.crm_cust_info cci
         left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid
         left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid;
