/*
  Data Warehouse Schema Setup (Intermediate)
  Warning: Drops existing bronze, silver, gold schemas
  Instructions:
    1. Create the database if not already done:
       create database sql_data_warehouse;
    2. Connect to 'sql_data_warehouse' using a client like
       DataGrip before running
*/

-- Restrict public access
revoke create on schema public from public;
revoke all on database sql_data_warehouse from public;

-- Drop existing schemas
drop schema if exists bronze cascade;
drop schema if exists silver cascade;
drop schema if exists gold cascade;

-- Create warehouse schemas
create schema bronze;
create schema silver;
create schema gold;