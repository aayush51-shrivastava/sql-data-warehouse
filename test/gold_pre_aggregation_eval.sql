/*
  Gold Layer Pre-Aggregation Evaluation
  Purpose: Validate and prepare aggregated data before creating Gold views
  Steps:
    1. Ensure Silver tables are populated
    2. Connect to 'sql_data_warehouse'
    3. Run checks below for customer and product integration, deduplication,
       and sales fact table validation
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

-- Product data integration across CRM and ERP (include all products)
select cpd.prd_id,
       cpd.cat_id,
       cpd.prd_key,
       cpd.prd_nm,
       cpd.prd_cost,
       cpd.prd_line,
       cpd.prd_start_dt,
       cpd.prd_end_dt
from silver.crm_prd_info cpd;

-- Master data rule: CRM product list enriched with ERP categories
select cpd.prd_id,
       cpd.cat_id,
       cpd.prd_key,
       cpd.prd_nm,
       cpd.prd_cost,
       cpd.prd_line,
       cpd.prd_start_dt,
       epc.cat,
       epc.subcat,
       epc.maintenance
from silver.crm_prd_info cpd
         left join silver.erp_px_cat_g1v2 epc on cpd.cat_id = epc.id;

-- Duplicate check: ensure join doesn’t create multiple rows per product
select sub.prd_key, count(*)
from (select cpd.prd_id,
             cpd.cat_id,
             cpd.prd_key,
             cpd.prd_nm,
             cpd.prd_cost,
             cpd.prd_line,
             cpd.prd_start_dt,
             epc.cat,
             epc.subcat,
             epc.maintenance
      from silver.crm_prd_info cpd
               left join silver.erp_px_cat_g1v2 epc on cpd.cat_id = epc.id) sub
group by sub.prd_key
having count(*) > 1;

-- Reference output: candidate query for creating gold-level product view
select cpd.prd_id product_id,
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

-- Building and evaluating the sales fact table
select csd.sls_ord_num,
       csd.sls_prd_key,
       csd.sls_cust_id,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price
from silver.crm_sales_details csd;

-- Fact sales integration with gold dimensions
select csd.sls_ord_num,
       dpr.product_number,  -- surrogate key from gold.dim_products
       dcs.customer_number, -- surrogate key from gold.dim_customers
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price
from silver.crm_sales_details csd
         left join gold.dim_products dpr on csd.sls_prd_key = dpr.product_key
         left join gold.dim_customers dcs on csd.sls_cust_id = dcs.customer_id;

-- Check for missing product mappings in fact sales
select csd.sls_ord_num,
       dpr.product_number,
       dcs.customer_number,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price
from silver.crm_sales_details csd
         left join gold.dim_products dpr on csd.sls_prd_key = dpr.product_key
         left join gold.dim_customers dcs on csd.sls_cust_id = dcs.customer_id
where dpr.product_number is null;

-- Check for missing customer mappings in fact sales
select csd.sls_ord_num,
       dpr.product_number,
       dcs.customer_number,
       csd.sls_order_dt,
       csd.sls_ship_dt,
       csd.sls_due_dt,
       csd.sls_sales,
       csd.sls_quantity,
       csd.sls_price
from silver.crm_sales_details csd
         left join gold.dim_products dpr on csd.sls_prd_key = dpr.product_key
         left join gold.dim_customers dcs on csd.sls_cust_id = dcs.customer_id
where dcs.customer_number is null;
