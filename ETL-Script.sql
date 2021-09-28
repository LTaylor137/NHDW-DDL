-- Student ID: 103200214
-- Name: Lachlan Taylor
-- 08/09/2021
-- github repo https://github.com/LTaylor137/NHDW-DDL



--------------------------------------------------------------------------------
----------------------------- General table lookups ----------------------------
--------------------------------------------------------------------------------


-- SELECT NAME FROM SYS.DATABASES;

-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- SELECT * FROM master.sys.sql_logins;

-- SELECT * FROM [DDDM_TPS_1].sys.sql_logins;


--------------------------------------------------------------------------------
-------------------------------- DataWarehouse ---------------------------------
--------------------------------------------------------------------------------

-- Ahlams
-- shareddb.chxrsmr071sd.us-east-1.rds.amazonaws.com
-- admin
-- password = melbourne123


-- Lachlans
-- nhrmdwldt.cyw97dursdgw.us-east-1.rds.amazonaws.com
-- instance name: NHRMDWLDT
-- username: admin
-- pass: Applejacks


CREATE DATABASE NHDW_LDT_0214;

USE NHDW_LDT_0214;

-- -- the below is needed to be run on the master level of the datawarehouse server 
-- -- so that people can login remotely.

use master;
GO

EXEC sp_configure 'show advanced options', 1;
-- RECONFIGURE;
GO

exec sp_configure 'Ad Hoc Distributed Queries', 1;
RECONFIGURE;  
GO

-- EXEC sp_configure 'xp_cmdshell', 1
-- RECONFIGURE
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
-- OPENROWSET can only accept strings, not variables.

SELECT *
FROM
    OPENROWSET('SQLNCLI', 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
'SELECT * FROM DDDM_TPS_1.dbo.PATIENT');



--------------------------------------------------------------------------------

-- execute the below while logged into your own DW server.




SELECT *
FROM
    OPENROWSET('SQLNCLI', 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
'SELECT * FROM DDDM_TPS_1.dbo.PATIENT
WHERE URNUMBER NOT IN (SELECT SOURCEID FROM DW_PATIENT)');




-- IF OBJECT_ID('ETL_DIM_PATIENT') IS NOT NULL
-- DROP PROCEDURE ETL_DIM_PATIENT;
-- GO

-- CREATE PROCEDURE ETL_DIM_PATIENT
-- AS

-- BEGIN

--     BEGIN TRY

--       SELECT *
--     FROM
--         OPENROWSET('SQLNCLI', 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
-- 'SELECT * FROM DDDM_TPS_1.dbo.PATIENT')
-- WHERE URNumber < 900000000;

--     END TRY

--     BEGIN CATCH



--     END CATCH;

-- END;


-- PRINT ETL_DIM_PATIENT;

-- EXEC ETL_DIM_PATIENT;

--------------------------------------------------------------------------------
------------------------------- Tims Solution ----------------------------------
--------------------------------------------------------------------------------


-- CREATE GET CONNECTION STRING FUNCTION.

-- :SETVAR SOURCEURL 10

DROP FUNCTION IF EXISTS GET_CONNECTION_STRING;
GO
CREATE FUNCTION GET_CONNECTION_STRING() RETURNS NVARCHAR(MAX) AS
BEGIN
    RETURN 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;';
END;
GO

-- CREATE A COMMAND STRING.

BEGIN
    DECLARE @COMMAND1 NVARCHAR(MAX);
    SET @COMMAND1 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT'');'
    -- PRINT(@COMMAND1);          
    EXEC(@COMMAND1);
END

-- CREATE A COMMAND STRING WITH WHERE LOGIC.

BEGIN
    DECLARE @COMMAND2 NVARCHAR(MAX);
    SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (900000001, 900000002)'');'
    -- PRINT(@COMMAND2);          
    EXEC(@COMMAND2);
END



-- CREATE A COMMAND STRING USING ROWNUMS.
-- SELECT * FROM LIST_OF_ROWNUMS;

BEGIN

-- GO AND GET THE CONNECTION STRING.
    DECLARE @CONNSTRING NVARCHAR(MAX);
    EXEC @CONNSTRING = GET_CONNECTION_STRING;

-- SET UP A STRING OF XXX TO EXCLUDE FROM COMMAND
    DECLARE @ROWNUMS NVARCHAR(MAX);
    SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
    FROM NHDW_LDT_0214.DBO.LIST_OF_ROWNUMS
    PRINT (@ROWNUMS);

--CREATE THE COMMAND TO SEND TO THE OTHER SERVER
    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''' + @CONNSTRING + ''',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
    -- PRINT(@COMMAND);          
    EXEC(@COMMAND);

END


-- BEGIN
--     DECLARE @COMMAND3 NVARCHAR(MAX);
--     SET @COMMAND3 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
--                     '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
--     -- PRINT(@COMMAND3);          
--     EXEC(@COMMAND3);
-- END






DROP PROCEDURE IF EXISTS DIM_PATIENT_TRANSFER_GOOD
AS

BEGIN
    -- get list of all patients not required -- patients already in dw -- patients in EE

    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.DW_PATIENT
    WHERE DWSOURCEBD = 'NHRM';
    PRINT @ALREADY_IN_DIM;

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    WHERE DWSOURCEBD = 'NHRM';
    --PRINT @IN_ERROR_EVENT;

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;
    PRINT @TO_EXCLUDE;

    -- write the coide to get the required data - excludes those identified above.
    DECLARE @CONNSTRING NVARCHAR(MAX);
    EXECUTE @CONNSTRING = GET_CONNECTION_STRING;

    DECLARE @SELECTQUERY NVARCHAR(MAX);
    SET @SELECTQUERY = '''SELECT URNUMBER, GENDER, YEAR(DOB) AS YOB,' +
                    'SUBURB, POSTCODE, COUNTRYOFBIRTH, LIVESALONE, ACTIVE, ' +
                    '(SELECT TOP 1 DIAGNOSIS FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER - P.URNUMBER) AS [DIAGNOSIS],' +

                    '(SELECT TOP 1 CATEGORYNAME FROM DDDM_TPS_1.DBO.PATIENTCATEGORY PC' +
                    ' INNER JOIN DDDM_TPS_1.DBO.TEMPLATECATEGORY TC' +
                    ' ON PC.CATEGORYID = TC.CATEGORYID' +
                    ' WHERE PC.URNUMBER = P.URNUMBER) AS [CATEGORY], ' +

                    '(SELECT TOP 1 PROCEDUREDATE FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER = P.URNUMBER) AS [PROCEDURE],' +
                    ' FROM DDDM_TPS_1.DBO.PATIENT P WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')''';




    DECLARE @INSERTQUERY NVARCHAR(MAX);
    SET @INSERTQUERY = 'INSERT INTO DW_PATIENT(DWPATIENTID, SOURCEDB, SOURCEID, GENDER, YOB)' +
                    'SELECT 1, ''NHRM'', SOURCE.URNUMBER, SOURCE.GENDER, SOURCE.YOB';

    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = @INSERTQUERY + ' FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNSTRING + ''',' + @SELECTQUERY + ') SOURCE;'

    --PRINT(@COMMAND);
    EXECUTE(@COMMAND);
END
