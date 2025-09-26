/*
  Gold Layer View Creation
  Purpose: Provide unified customer and product profiles by aggregating and
   reconciling CRM and ERP data
  Steps:
    1. Ensure Silver tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Execute to create the views
*/

-- Drop existing views if they exist
drop view if exists gold.dim_customers cascade;
drop view if exists gold.dim_products cascade;
drop view if exists gold.fact_sales cascade;

-- Create customer dimension view
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

-- Create product dimension view
create view gold.dim_products as
select row_number() over (order by cpd.prd_start_dt, cpd.prd_id)
           product_number, -- surrogate number
       cpd.prd_id product_id,
       cpd.prd_key product_key,
       cpd.prd_nm product_name,
       cpd.cat_id category_id,
       epc.cat "category",
       epc.subcat sub_category,
       epc.maintenance,
       cpd.prd_cost "cost",
       cpd.prd_line product_line,
       cpd.prd_start_dt start_date,
       cpd.prd_end_dt end_date
from silver.crm_prd_info cpd
         left join silver.erp_px_cat_g1v2 epc on cpd.cat_id = epc.id;

-- Building gold.fact_sales view
create view gold.fact_sales as
select csd.sls_ord_num order_number,
       dpr.product_number,  -- surrogate key from gold.dim_products
       dcs.customer_number, -- surrogate key from gold.dim_customers
       csd.sls_order_dt order_date,
       csd.sls_ship_dt shipping_date,
       csd.sls_due_dt due_date,
       csd.sls_sales sales_amount,
       csd.sls_quantity quantity,
       csd.sls_price price
from silver.crm_sales_details csd
         left join gold.dim_products dpr on csd.sls_prd_key = dpr.product_key
         left join gold.dim_customers dcs on csd.sls_cust_id = dcs.customer_id;