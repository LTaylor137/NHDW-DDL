-- Student ID: 103200214
-- Name: Lachlan Taylor
-- 08/09/2021
-- github repo https://github.com/LTaylor137/NHDW-DDL



--------------------------------------------------------------------------------
----------------------------- General table lookups ----------------------------
--------------------------------------------------------------------------------

-- SELECT NAME FROM SYS.DATABASES;

-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

--------------------------------------------------------------------------------
-------------------------- Bens DataWarehouse Database -------------------------
--------------------------------------------------------------------------------

-- northernhospitaldatawarehouse-1272.cd64zuwe3yii.us-east-1.rds.amazonaws.com
-- admin
-- Fenix7743Lo

-- CREATE DATABASE NHDW_LDT_0214;

-- USE NHDW_LDT_0214;

-- the below is needed to be run on the master level of the datawarehouse server 
-- so that people can login remotely.
-- use master;
-- exec sp_configure 'show advanced options', 1;  
-- --RECONFIGURE;
-- GO 
-- exec sp_configure 'Ad Hoc Distributed Queries', 1;  
-- --RECONFIGURE;  
-- GO  

--------------------------------------------------------------------------------
------------------------------- Source Database --------------------------------
--------------------------------------------------------------------------------

-- dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com
-- Database:  DDDM_TPS_1
-- admin
-- Kitemud$41

-- Drop user ldtjobmanager;

-- Drop login ldtjobmanager;

use master;
GO

CREATE LOGIN ldtreadonly WITH PASSWORD = 'Kitemud$41';
GO

USE [DDDM_TPS_1];
GO

CREATE USER ldtreadonly FOR LOGIN ldtreadonly;
GO

EXEC sp_addrolemember [db_datareader], ldtreadonly;

-- note sp_addrolemember is a "stored proceedure"

--------------------------------------------------------------------------------

-- execute the below while logged into Bens DW server.
-- if this doesnt work

SELECT *
FROM
OPENROWSET('SQLNCLI', 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
'SELECT * FROM DDDM_TPS_1.dbo.PATIENT');







-- SELECT * FROM master.sys.sql_logins;

-- SELECT * FROM [DDDM_TPS_1].sys.sql_logins;

-- SELECT NAME FROM SYS.DATABASES;

-- SELECT * FROM INFORMATION_SCHEMA.TABLES;



















