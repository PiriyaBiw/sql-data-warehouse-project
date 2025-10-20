/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. Moreover, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
*/

-- Create The 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

-- Then “Connect” to The 'DataWarehouse' database
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
