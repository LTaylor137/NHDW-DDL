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

-- USE DDDM_TPS_1

-- SELECT * FROM PATIENT

--------------------------------------------------------------------------------
-------------------------------- DataWarehouse ---------------------------------
--------------------------------------------------------------------------------

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
--------------------------- Tasks to complete ----------------------------------
--------------------------------------------------------------------------------

-- Problem 1 Piecing together our query to exclude data already in the DW and EE.
-- Problem 2 Get the required data from the source.
-- Problem 3 store data (in a non permanent way i.e memory) to pass between various ETL procedures

------- the below are stored procedures that you pass the data to use as a parameter

-- Problem 4 apply any filters to the data.
-- Problem 5 insert the good data
-- Problem 6 insert any data which the filter rules say needs to be transformed.

--------------------------------------------------------------------------------
------------------------------- Tims Solution ----------------------------------
--------------------------------------------------------------------------------

-- Problem 2 Get the required data from the source.


-- EXAMPLE CREATE GET CONNECTION STRING FUNCTION.
USE NHDW_LDT_0214;

DROP FUNCTION IF EXISTS GET_CONNECTION_STRING;
GO
CREATE FUNCTION GET_CONNECTION_STRING() RETURNS NVARCHAR(MAX) AS
BEGIN
    RETURN 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;';
END;
GO

-- EXAMPLE CREATE A COMMAND STRING.
BEGIN
    DECLARE @COMMAND1 NVARCHAR(MAX);

    SET @COMMAND1 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT'');'
    -- PRINT(@COMMAND1);          
    EXEC(@COMMAND1);
END

-- EXAMPLE CREATE A COMMAND STRING WITH WHERE LOGIC.
-- BEGIN
--     DECLARE @COMMAND2 NVARCHAR(MAX);
--     SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
--                     '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (900000001, 900000002)'');'
--     -- PRINT(@COMMAND2);          
--     EXEC(@COMMAND2);
-- END

-- Problem 1 Piecing together our query to exclude data already in the DW and EE.
-- EXAMPLE CREATE A COMMAND STRING USING ROWNUMS.
BEGIN

    -- GO AND GET THE CONNECTION STRING.
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

    -- SET UP A STRING OF ID NUMBERS TO EXCLUDE FROM COMMAND (i.e. numbers already existing in EE or DW.)
    DECLARE @ROWNUMS NVARCHAR(MAX);
    SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
    FROM NHDW_LDT_0214.DBO.LIST_OF_ROWNUMS
    PRINT (@ROWNUMS);

    --CREATE THE COMMAND TO SEND TO THE OTHER SERVER
    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''' + @CONNECTIONSTRING + ''',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
    PRINT(@COMMAND);
    EXEC(@COMMAND);

END

------------------ TEMPORARY TABLE METHOD ------------------

-- THE SCOPE OF THE VARIABLE ENDS WHEN THE SESSION DOES.

-- BEGIN
--     DROP TABLE IF EXISTS #TEMPTABLE;

--     SELECT *
--     INTO #TEMPTABLE
--     FROM PATIENT;

--     SELECT 1, *
--     FROM #TEMPTABLE;
-- END;






-- Problem 3 store data (in a non permanent way i.e memory) 
-- to pass between various ETL procedures

------------------ STORED PROCEDURE METHOD ------------------

USE NHDW_LDT_0214;

DROP TYPE IF EXISTS TEMPTABLETYPE;
DROP PROCEDURE IF EXISTS TABLE_PARAM_TEST;
DROP PROCEDURE IF EXISTS GETTEMPDATA;
DROP PROCEDURE IF EXISTS VAR_SELECT_TEST;


--CREATE A DATA TYPE
CREATE TYPE TEMPTABLETYPE AS TABLE
(
    TESTID INT,
    TESTDATA NVARCHAR(100)
)
GO

-- DECLARE A VARIABLE, TO STORE THE RESULTS OF A SELECT.
-- THE SCOPE OF THE VARIABLE ENDS WHEN THE VARIABLE DOES.
-- 28:52MINS

--THIS PROCEDURE CREATES THE TEMPORARY TABLE
CREATE PROCEDURE TABLE_PARAM_TEST
AS
BEGIN
    SELECT 'ZZZ', *
    FROM #TEMPTABLE;
END;
GO

-- THIS PROCEDURE GETS THE DATA AND PLACES IT INTO XXX  VERSION 2
CREATE PROCEDURE GETTEMPDATA
AS
BEGIN

    DROP TABLE IF EXISTS #TEMPTABLE;

    -- CREATE CONNECTION STRING
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''' + @CONNECTIONSTRING + ''',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT'');'

    SELECT * INTO #TEMPTABLE

    EXEC(@COMMAND);
END;

GO

CREATE PROCEDURE VAR_SELECT_TEST
AS
BEGIN

    -- CREATE CONNECTION STRING
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

    EXEC GETTEMPDATA;
    EXEC TABLE_PARAM_TEST;

END;

-- EXECUTE AFTER INITIALISING PROCEDURES
DROP TABLE IF EXISTS #TEMPTABLE;
GO
EXEC VAR_SELECT_TEST;
GO;

----+++++++++++++++++++++++++++++ THIS WAS WORKING.

-- -- THIS PROCEDURE GETS THE DATA AND PLACES IT INTO XXX   VERSION 1
-- CREATE PROCEDURE VAR_SELECT_TEST
-- AS
-- BEGIN

--     DECLARE @TEMPTABLE TEMPTABLETYPE;
--     -- THINK OF THIS AS A TEMPLATE.
--     SELECT *
--     FROM @TEMPTABLE;
--     -- CHECK CONTENTS OF TABLE BEFORE.

--     -- CREATE CONNECTION STRING
--     DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
--     EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

--     -- SET UP A STRING OF ID NUMBERS TO EXCLUDE FROM COMMAND (i.e. numbers already existing in EE or DW.)
--     -- DECLARE @ROWNUMS NVARCHAR(MAX);
--     -- SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
--     -- FROM NHDW_LDT_0214.DBO.LIST_OF_ROWNUMS
--     -- PRINT (@ROWNUMS);

--     DECLARE @COMMAND NVARCHAR(MAX);
--     SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''' + @CONNECTIONSTRING + ''',' +
--                     -- '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
--                     -- PULLS BASIC DATA 
--                     '''SELECT URNumber, FirstName FROM DDDM_TPS_1.dbo.PATIENT'');'

--     -- PRINT(@COMMAND);
--     INSERT INTO @TEMPTABLE
--     EXEC(@COMMAND);

--     EXEC TABLE_PARAM_TEST @IN_TABLE = @TEMPTABLE;
--     -- E.G. ETL_NHRM_PATIENT_FILTER_1

--     SELECT *
--     FROM @TEMPTABLE;
-- -- CHECK CONTENTS OF TABLE AFTER.

-- END;

-- -- EXECUTE AFTER INITIALISING PROCEDURES
-- EXEC VAR_SELECT_TEST

----+++++++++++++++++++++++++++++

-- SELECT *
-- FROM @TEMPTABLE;



-- BEGIN

--     DECLARE @PATIENTTABLE DEMOTABLETYPE;

--     INSERT INTO @PATIENTTABLE
--     SELECT URNUMBER, EMAIL, TITLE
--     FROM PATIENT;

--     SELECT 4, *
--     FROM @PATIENTTABLE;

-- END


--------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
-- get list of all patients not required -- patients already in dw -- patients in EE --
-----------------------------------------------------------------------------------------

-- SEE 42:50 MINS

SELECT *
FROM DW_PATIENT

DROP PROCEDURE IF EXISTS DIM_PATIENT_TRANSFER_GOOD
AS

BEGIN


    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.DW_PATIENT
    WHERE DWSOURCEDB = 'NHRM';
    --PRINT @ALREADY_IN_DIM;

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    WHERE DWSOURCEDB = 'NHRM';
    --PRINT @IN_ERROR_EVENT;

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;
    PRINT @TO_EXCLUDE;

    -- write the code to get the required data - excludes those identified above.
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

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


    -- END

    DECLARE @INSERTQUERY NVARCHAR(MAX);
    SET @INSERTQUERY = 'INSERT INTO DW_PATIENT(DWPATIENTID, SOURCEDB, SOURCEID, GENDER, YOB)' +
                    'SELECT 1, ''NHRM'', SOURCE.URNUMBER, SOURCE.GENDER, SOURCE.YOB';

    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = @INSERTQUERY + ' FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY + ') SOURCE;'

    --PRINT(@COMMAND);
    EXECUTE(@COMMAND);
END
