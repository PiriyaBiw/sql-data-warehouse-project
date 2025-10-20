/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. Moreover, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.

PostgreSQL / pgAdmin Notes:
    - PostgreSQL does not support the `USE` statement to switch databases.
      Instead, you must change the connection to the target database.
      (In pgAdmin: right-click the database → Connect or open a new Query Tool on the target database)
    - PostgreSQL does not recognize `GO`; use semicolon `;` to terminate statements.
    - Therefore, after creating the database, you must stop execution here,
      then connect to the 'DataWarehouse' database to create the schemas.
    - Example: In pgAdmin, after creating 'DataWarehouse', right-click → Query Tool
      → then run the schema creation commands.
*/

-- Create The 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

-- Then “Connect” to The 'DataWarehouse' database
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
