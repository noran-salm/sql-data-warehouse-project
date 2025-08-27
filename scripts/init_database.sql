-- Use Master Database
USE master;

-- Drop DataWarehouse If Exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create New DataWarehouse
CREATE DATABASE DataWarehouse;

-- Switch To DataWarehouse
USE DataWarehouse;

-- Create Medallion Schemas: Bronze / Silver / Gold
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
