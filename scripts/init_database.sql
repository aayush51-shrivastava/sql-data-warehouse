/*
  Data Warehouse Schema Setup
  Warning: Drops existing bronze, silver, gold schemas
  Steps:
    1. create database sql_data_warehouse;
    2. connect to sql_data_warehouse before running
*/

-- Restrict public access
revoke create on schema public from public;
revoke all on database sql_data_warehouse from public;

-- Drop old schemas
drop schema if exists bronze cascade;
drop schema if exists silver cascade;
drop schema if exists gold cascade;

-- Create schemas (bronze = raw, silver = cleaned, gold = curated)
create schema bronze;
create schema silver;
create schema gold;