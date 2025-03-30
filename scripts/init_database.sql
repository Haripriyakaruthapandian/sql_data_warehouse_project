USE Master;

if EXISTS (SELECT 1 FROM SYS.databases where name='DataWarehouse')
Begin
alter database DataWarehouse set SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;


CREATE DATABASE DataWarehouse;

Use DataWarehouse;

CREATE SCHEMA bronze;
Go
CREATE SCHEMA SILVER;
GO
CREATE SCHEMA GOLD;

