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

SELECT *
FROM sys.objects

--------------------------------------------------------------------------------
-------------------------------- DataWarehouse ---------------------------------
--------------------------------------------------------------------------------

-- Lachlans
-- nhrmdwldt.cyw97dursdgw.us-east-1.rds.amazonaws.com
-- instance name: NHRMDWLDT
-- username: admin
-- pass: Applejacks

-- CREATE DATABASE NHDW_LDT_0214;

-- USE NHDW_LDT_0214;

--------------------------------------------------------------------------------
--------------------- Permissions to execute in Source Database ----------------
--------------------------------------------------------------------------------

-- dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com
-- Database:  DDDM_TPS_1
-- admin
-- Kitemud$41

-- -- the below is needed to be run on the master level of the datawarehouse server 
-- -- so that people can login remotely.

-- use master;
-- GO

-- EXEC sp_configure 'show advanced options', 1;
-- -- RECONFIGURE;
-- GO

-- exec sp_configure 'Ad Hoc Distributed Queries', 1;
-- RECONFIGURE;  
-- GO

-- EXEC sp_configure 'xp_cmdshell', 1
-- RECONFIGURE
-- GO

-- Drop user ldtjobmanager;

-- Drop login ldtjobmanager;

-- use master;
-- GO

-- CREATE LOGIN ldtreadonly WITH PASSWORD = 'Kitemud$41';
-- GO

-- USE [DDDM_TPS_1];
-- GO

-- CREATE USER ldtreadonly FOR LOGIN ldtreadonly;
-- GO

-- EXEC sp_addrolemember [db_datareader], ldtreadonly;

-- note sp_addrolemember is a "stored proceedure"

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------- Tasks to complete ----------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------




-- Problem 1 Piecing together our query to exclude data already in the DW and EE.
-- Problem 2 Get the required data from the source.
-- Problem 3 store data (in a non permanent way i.e memory) to pass between various ETL procedures

------- the below are stored procedures that you pass the data to use as a parameter

-- Problem 4 apply any filters to the data.
-- Problem 5 insert the good data
-- Problem 6 insert any data which the filter rules say needs to be transformed.






--------------------------------------------------------------------------------
------------------------ Tims Solutions and examples ---------------------------
--------------------------------------------------------------------------------





-- execute the below while logged into your own DW server.
-- OPENROWSET can only accept strings, not variables.

SELECT *
FROM
    OPENROWSET('SQLNCLI', 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
'SELECT * FROM DDDM_TPS_1.dbo.PATIENT');

SELECT *
FROM
    OPENROWSET('SQLNCLI', 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
'SELECT * FROM DDDM_TPS_1.dbo.PATIENT
WHERE URNUMBER NOT IN (SELECT SOURCEID FROM DW_PATIENT)');

-- EXAMPLE OF CREATE A COMMAND STRING.

BEGIN
    DECLARE @COMMAND1 NVARCHAR(MAX);

    SET @COMMAND1 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT'');'
    -- PRINT(@COMMAND1);          
    EXEC(@COMMAND1);
END

-- EXAMPLE CREATE A COMMAND STRING WITH WHERE LOGIC.

BEGIN
    DECLARE @COMMAND2 NVARCHAR(MAX);
    SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
                    '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (900000001, 900000002)'');'
    PRINT('---- this is the command ----  ' + @COMMAND2);
    EXEC(@COMMAND2);
END

GO


-- EXAMPLE CREATE A COMMAND STRING USING ROWNUMS.

-- GO AND GET A LIST OF ID'S THAT EXIST IN THE ERROR EVENT TABLE, AND THE DW_PATIENT TABLE.
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


-----------------------------------------------------------------------------------------------------------
------------------------------------------ insert test data -----------------------------------------------
-----------------------------------------------------------------------------------------------------------

-- SEE 42:50 MINS


INSERT INTO ERROR_EVENT
VALUES
    (
        900000005, 'NHRM', 'TABLE', 1, GETDATE(), 'SKIP'
),
    (
        900000010, 'NHRM', 'TABLE', 1, GETDATE(), 'SKIP'
)

INSERT INTO DW_PATIENT
VALUES
    (
        900000015, 900000015, 'NHRM', 'TABLE', 'Male', GETDATE(), 'Suburb', 3000, 'Australia', '0', +
        '1', 1, 'IPC', GETDATE(), 'Bad'
),
    (
        900000020, 900000020, 'NHRM', 'TABLE', 'Female', GETDATE(), 'Suburb', 3000, 'Australia', '0', +
        '1', 1, 'IPC', GETDATE(), 'Bad'
)

SELECT *
FROM DW_PATIENT

SELECT *
FROM ERROR_EVENT
-- test data

go;



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
----------------- Problem 1 Piecing together our query to exclude data already in the DW and EE. -----------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------



-- get list of all patients not required -- patients already in dw -- patients in EE 
-- e.g. "(900000015,900000020,900000005,900000010)"


-- TODO this doesnt work if DW_PATIENT contains no ID's


SELECT *
FROM DW_PATIENT

SELECT *
FROM ERROR_EVENT
-- test data



USE NHDW_LDT_0214

DROP PROCEDURE IF EXISTS GET_IDS_TO_EXCLUDE
GO
CREATE PROCEDURE GET_IDS_TO_EXCLUDE
AS
BEGIN

    -- GET THE 
    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.DW_PATIENT
    -- WHERE DWSOURCEDB = 'NHDW_LDT_0214';
    PRINT @ALREADY_IN_DIM;

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    -- WHERE DWSOURCEDB = 'NHDW_LDT_0214';
    PRINT @IN_ERROR_EVENT;

    -- resolves issue if @ALREADY_IN_DIM contained no values, then @TO_EXCLUDE would not SET at all.
    IF (@ALREADY_IN_DIM IS NULL)
    BEGIN
        SET @ALREADY_IN_DIM = '0'
    END

    IF (@IN_ERROR_EVENT IS NULL)
    BEGIN
        SET @IN_ERROR_EVENT = '0'
    END

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = '(' + @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT + ')';

    print 'List of IDs to exclude ' + CHAR(13)+CHAR(10) + @TO_EXCLUDE;

-- DECLARE @COMMAND2 NVARCHAR(MAX);
-- SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                 '''Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
--                 '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN ''' + @EXISTING_PATIENT_IDS + ''');'        
-- EXEC(@COMMAND2);
END;

EXEC GET_IDS_TO_EXCLUDE;



------------------------------------------------------------------------------------------------------------------------
--------------------------------- Problem 2 Get the required data from the source. -------------------------------------
------------------------------------------------------------------------------------------------------------------------



GO

-- CREATE GET CONNECTION STRING FUNCTION.
USE NHDW_LDT_0214;

DROP FUNCTION IF EXISTS GET_CONNECTION_STRING;
GO
CREATE FUNCTION GET_CONNECTION_STRING() RETURNS NVARCHAR(MAX) AS
BEGIN
    RETURN 'Server=dad.cbrifzw8clzr.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;';
END;
GO



------------------ TEMPORARY TABLE METHOD ------------------

USE NHDW_LDT_0214;

GO

-- Using a temporary table method.

-- THIS PROCEDURE GETS THE DATA AND PLACES IT INTO XXX  VERSION 2
DROP PROCEDURE IF EXISTS GETTEMPDATA;
GO
CREATE PROCEDURE GETTEMPDATA
AS
BEGIN

    DROP TABLE IF EXISTS TEMPTABLE1;
    CREATE TABLE TEMPTABLE1
    (
        TESTID INT,
        TESTDATA NVARCHAR(100)
    )

    -- CREATE CONNECTION STRING
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = 'INSERT INTO TEMPTABLE SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''' + @CONNECTIONSTRING + ''',' +
                    '''SELECT URNumber, SurName FROM DDDM_TPS_1.dbo.PATIENT'');'

    PRINT 'BEFORE'

    SELECT *
    FROM TEMPTABLE1

    EXEC(@COMMAND);

    PRINT 'AFTER'

    SELECT *
    FROM TEMPTABLE1

END;

GO

DROP PROCEDURE IF EXISTS VAR_SELECT_TEST;
GO
CREATE PROCEDURE VAR_SELECT_TEST
AS
BEGIN
    EXEC GETTEMPDATA;
END;

-- EXECUTE AFTER INITIALISING PROCEDURES
EXEC VAR_SELECT_TEST;



-----------------------------------------------------------------------------------------------------------------------------
------------- Problem 3 store data (in a non permanent way i.e memory) to pass between various ETL procedures ---------------
-----------------------------------------------------------------------------------------------------------------------------



-- pull data from DDDM_TPS_1.DBO.PATIENT and insert it into a temptable. 
DROP PROCEDURE IF EXISTS TRANSFER_DATA_TO_TEMP_STORAGE
GO
CREATE PROCEDURE TRANSFER_DATA_TO_TEMP_STORAGE
AS
BEGIN

    -- get a string of id's already in EE and DW tables.
    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.DW_PATIENT
    -- WHERE DWSOURCEDB = 'NHRM';

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    -- WHERE DWSOURCEDB = 'NHRM';

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;
    PRINT @TO_EXCLUDE;

    -- get connection string
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

    DROP TABLE IF EXISTS TEMPTABLE;
    CREATE TABLE TEMPTABLE
    (
        URNUMBER INT,
        GENDER NVARCHAR(10),
        DOB INT,
        SUBURB NVARCHAR(MAX),
        POSTCODE NVARCHAR(4),
        COUNTRYOFBIRTH NVARCHAR(MAX),
        LIVESALONE NVARCHAR(1),
        ACTIVE NVARCHAR(1),
        [DIAGNOSIS] NVARCHAR(MAX),
        [CATEGORY] NVARCHAR(MAX),
        [PROCEDURE] NVARCHAR(MAX)
    )

    DECLARE @INSERTQUERY NVARCHAR(MAX);
    SET @INSERTQUERY = 'INSERT INTO TEMPTABLE SELECT URNUMBER,Gender,DOB,Suburb,postcode,CountryOfBirth,LIVESALONE,Active,DIAGNOSIS,CATEGORY,[PROCEDURE]'

    -- write the code to get the required data - excludes those identified above.
    DECLARE @SELECTQUERY NVARCHAR(MAX);
    SET @SELECTQUERY = '''SELECT URNUMBER, GENDER, YEAR(DOB) AS DOB,' +
                    'SUBURB, POSTCODE, COUNTRYOFBIRTH, LIVESALONE, ACTIVE, ' +
                    '(SELECT TOP 1 DIAGNOSIS FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER = P.URNUMBER) AS [DIAGNOSIS],' +

                    '(SELECT TOP 1 CATEGORYNAME FROM DDDM_TPS_1.DBO.PATIENTCATEGORY PC' +
                    ' INNER JOIN DDDM_TPS_1.DBO.TEMPLATECATEGORY TC' +
                    ' ON PC.CATEGORYID = TC.CATEGORYID' +
                    ' WHERE PC.URNUMBER = P.URNUMBER) AS [CATEGORY], ' +

                    '(SELECT TOP 1 PROCEDUREDATE FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER = P.URNUMBER) AS [PROCEDURE]' +
                    ' FROM DDDM_TPS_1.DBO.PATIENT P WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')''';

    DECLARE @COMMAND NVARCHAR(MAX);
    SET @COMMAND = @INSERTQUERY + ' FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY + ');'

    SELECT *
    FROM TEMPTABLE
    -- view table before 

    PRINT('---- this is the command ----  ' + @COMMAND);
    EXECUTE(@COMMAND);

    SELECT *
    FROM NHDW_LDT_0214.DBO.TEMPTABLE
-- view table after 

END;

EXEC TRANSFER_DATA_TO_TEMP_STORAGE;

SELECT *
FROM DW_PATIENT

SELECT *
FROM TEMPTABLE

SELECT *
FROM NHDW_LDT_0214.DBO.ERROR_EVENT

----------------------------------------------------------------------------------------
----------------------------------- Apply Filters --------------------------------------
----------------------------------------------------------------------------------------
-- Problem 4 apply any filters to the data.



-- Gender is not Male or Female
DROP PROCEDURE IF EXISTS FILTER_1_GENDER
GO
CREATE PROCEDURE FILTER_1_GENDER
AS
BEGIN

    INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE,
        SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT TT.URNUMBER, 'NHRM', 'TABLE', '1', SYSDATETIME(), 'MODIFY'
    FROM NHDW_LDT_0214.DBO.TEMPTABLE TT
    WHERE TT.GENDER NOT IN ('Male', 'Female');

    DELETE 
    FROM NHDW_LDT_0214.DBO.TEMPTABLE
    WHERE GENDER NOT IN ('Male', 'Female');

END

-- Title is not Mrs., Ms., Mr.

DROP PROCEDURE IF EXISTS FILTER_2_TITLE
GO
CREATE PROCEDURE FILTER_2_TITLE
AS
BEGIN

    INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE,
        SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT TT.URNUMBER, 'NHRM', 'TABLE', '2', SYSDATETIME(), 'MODIFY'
    FROM NHDW_LDT_0214.DBO.TEMPTABLE TT
    WHERE TT.GENDER NOT IN ('Mr.', 'Ms.', 'Mrs.');

    DELETE 
    FROM NHDW_LDT_0214.DBO.TEMPTABLE 
    WHERE GENDER NOT IN ('Mr.', 'Ms.', 'Mrs.');

END


-- Postcode is not 4 numbers long.
DROP PROCEDURE IF EXISTS FILTER_3_POSTCODE
GO
CREATE PROCEDURE FILTER_3_POSTCODE
AS
BEGIN

    INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE,
        SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT TT.URNUMBER, 'NHRM', 'TABLE', '1', SYSDATETIME(), 'SKIP'
    FROM NHDW_LDT_0214.DBO.TEMPTABLE TT
    WHERE LEN(TT.POSTCODE) != 3;

    DELETE 
    FROM NHDW_LDT_0214.DBO.TEMPTABLE
    WHERE LEN(POSTCODE) != 4;

END

-- MobileNumber is not 10 numbers long.
SELECT * FROM DW0214.DBO.ERROREVENT;
GO

INSERT INTO DW0214.DBO.ERROREVENT (ERRORID, SOURCE_ID, SOURCE_TABLE, FILTERID, DATETIME, ACTION)

SELECT NEXT VALUE FOR NEWERRORID_SEQ, CUSTID, 'CUSTBRIS', 5, GETDATE(), 'MODIFY'
FROM TPS.DBO.CUSTBRIS
WHERE Phone LIKE '% %' 
OR Phone LIKE '%-%';
GO


-- MobileNumber is not 10 numbers long.

-- Category is null 

----------------------------------------------------------------------------------------
------------------------------- Transfer into DW PATIENT -------------------------------
----------------------------------------------------------------------------------------
-- Problem 5 insert the good data



go
USE NHDW_LDT_0214;

DROP PROCEDURE IF EXISTS TRANSFER_DATA_TO_DW_PATIENT_TABLE
GO
CREATE PROCEDURE TRANSFER_DATA_TO_DW_PATIENT_TABLE
AS
BEGIN

    INSERT INTO NHDW_LDT_0214.DBO.DW_PATIENT
        (
        DWPATIENTID,
        URNUMBER,
        DWSOURCEDB,
        DWSOURCETABLE,
        GENDER,
        DOB,
        SUBURB,
        POSTCODE,
        COUNTRYOFBIRTH,
        LIVESALONE,
        ACTIVE,
        CATEGORYID,
        CATEGORYNAME,
        PROCEDUREDATE,
        DIAGNOSIS)
    SELECT
        TT.URNUMBER,
        TT.URNUMBER,
        'NRHM',
        'Patient',
        TT.GENDER,
        GETDATE(),
        TT.SUBURB,
        TT.POSTCODE,
        TT.COUNTRYOFBIRTH,
        TT.LIVESALONE,
        TT.ACTIVE,
        '1',
        'CATEGORY',
        GETDATE(),
        'DIAGNOSIS'
    FROM NHDW_LDT_0214.DBO.TEMPTABLE TT

END

EXEC TRANSFER_DATA_TO_DW_PATIENT_TABLE





-- -------------------------Using a variable datatype method.---------------------------------



-- CREATE A DATA TYPE
DROP TYPE IF EXISTS TESTINGTABLETYPE;
    GO
CREATE TYPE TESTINGTABLETYPE AS TABLE
    (
    URNUMBER INT,
    GENDER NVARCHAR(10),
    DOB INT,
    SUBURB NVARCHAR(MAX),
    POSTCODE NVARCHAR(4),
    COUNTRYOFBIRTH NVARCHAR(MAX),
    -- PreferredLanguage NVARCHAR(MAX),
    LIVESALONE NVARCHAR(1),
    ACTIVE NVARCHAR(1),
    [DIAGNOSIS] NVARCHAR(MAX),
    [CATEGORY] NVARCHAR(MAX),
    [PROCEDURE] NVARCHAR(MAX)
    );

    GO

DECLARE @PATIENTTABLE TESTINGTABLETYPE;



go

-- THIS PROCEDURE GETS THE DATA AND PLACES IT INTO XXX   VERSION 1

DROP PROCEDURE IF EXISTS VAR_SELECT_TEST;
GO
CREATE PROCEDURE VAR_SELECT_TEST
AS
BEGIN


    --CREATE A DATA TYPE
    -- DROP TYPE IF EXISTS TEMPTABLETYPE;
    -- GO
    -- CREATE TYPE TEMPTABLETYPE AS TABLE
    -- (
    --     TESTID INT,
    --     TESTDATA NVARCHAR(100)
    -- )
    -- GO

    -- DECLARE A VARIABLE, TO STORE THE RESULTS OF A SELECT. THE SCOPE OF THE VARIABLE ENDS WHEN THE VARIABLE DOES. -- 28:52MINS

    --THIS PROCEDURE CREATES THE TEMPORARY TABLE
    -- CREATE PROCEDURE TABLE_PARAM_TEST @IN_TABLE TEMPTABLETYPE READONLY
    -- AS
    -- BEGIN
    --     SELECT 'ZZZ', *
    --     FROM @IN_TABLE
    -- END;


    DROP TABLE IF EXISTS TEMPTABLE;
go
DECLARE @TEMPTABLEVAR TEMPTABLETYPE;
-- THINK OF THIS AS A TEMPLATE.
SELECT *
FROM @TEMPTABLEVAR;
-- CHECK CONTENTS OF TABLE BEFORE.

-- CREATE CONNECTION STRING
DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

-- SET UP A STRING OF ID NUMBERS TO EXCLUDE FROM COMMAND (i.e. numbers already existing in EE or DW.)
-- DECLARE @ROWNUMS NVARCHAR(MAX);
-- SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
-- FROM NHDW_LDT_0214.DBO.LIST_OF_ROWNUMS
-- PRINT (@ROWNUMS);

DECLARE @COMMAND NVARCHAR(MAX);
SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''' + @CONNECTIONSTRING + ''',' +
                    -- '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
                    -- PULLS BASIC DATA 
                    '''SELECT URNumber, FirstName FROM DDDM_TPS_1.dbo.PATIENT'');'

-- PRINT(@COMMAND);
INSERT INTO @TEMPTABLEVAR
EXEC(@COMMAND);

EXEC TABLE_PARAM_TEST @IN_TABLE = @TEMPTABLEVAR;
-- E.G. ETL_NHRM_PATIENT_FILTER_1

SELECT *
FROM @TEMPTABLEVAR;
-- CHECK CONTENTS OF TABLE AFTER.

END;

-- -- EXECUTE AFTER INITIALISING PROCEDURES
-- EXEC VAR_SELECT_TEST

----+++++++++++++++++++++++++++++

-- SELECT *
-- FROM @TEMPTABLEVAR;



-- BEGIN

--     DECLARE @PATIENTTABLE DEMOTABLETYPE;

--     INSERT INTO @PATIENTTABLE
--     SELECT URNUMBER, EMAIL, TITLE
--     FROM PATIENT;

--     SELECT 4, *
--     FROM @PATIENTTABLE;

-- END


--------------------------------------------------------------------------------

-- keeping working code here.


-- DROP PROCEDURE IF EXISTS DIM_PATIENT_TRANSFER_GOOD
-- GO
-- CREATE PROCEDURE DIM_PATIENT_TRANSFER_GOOD
-- AS
-- BEGIN

--     -- get a string of id's already in EE and DW tables.
--     DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
--     SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
--     FROM NHDW_LDT_0214.DBO.DW_PATIENT
--     -- WHERE DWSOURCEDB = 'NHRM';
--     --PRINT @ALREADY_IN_DIM;

--     DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
--     SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
--     FROM NHDW_LDT_0214.DBO.ERROR_EVENT
--     -- WHERE DWSOURCEDB = 'NHRM';
--     -- PRINT @IN_ERROR_EVENT;

--     DECLARE @TO_EXCLUDE NVARCHAR(MAX)
--     SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;
--     PRINT @TO_EXCLUDE;

-- -- get connection string
--     DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
--     EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

--     DROP TABLE IF EXISTS TEMPTABLE;
--     CREATE TABLE TEMPTABLE
--     (
--         TESTID INT,
--         TESTDATA NVARCHAR(100)
--     )

--     DECLARE @INSERTQUERY NVARCHAR(MAX);
--     -- SET @INSERTQUERY = 'INSERT INTO DW_PATIENT(DWPATIENTID, URNUMBER, DWSOURCEDB, DWSOURCETABLE, GENDER, DOB, SUBURB, POSTCODE, COUNTRYOFBIRTH,' +
--     --                     'PREFFEREDLANGUAGE, LIVESALONE, ACTIVE, CATEGORYID, CATEGORYNAME, PROCEDUREDATE, DIAGNOSIS)'
--     SET @INSERTQUERY = 'INSERT INTO TEMPTABLE SELECT *'

--     -- write the code to get the required data - excludes those identified above.
--     DECLARE @SELECTQUERY NVARCHAR(MAX);
--     -- SET @SELECTQUERY = '''SELECT URNUMBER, GENDER, YEAR(DOB) AS YOB,' +
--     --                 'SUBURB, POSTCODE, COUNTRYOFBIRTH, LIVESALONE, ACTIVE, ' +
--     --                 '(SELECT TOP 1 DIAGNOSIS FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER - P.URNUMBER) AS [DIAGNOSIS],' +

--     --                 '(SELECT TOP 1 CATEGORYNAME FROM DDDM_TPS_1.DBO.PATIENTCATEGORY PC' +
--     --                 ' INNER JOIN DDDM_TPS_1.DBO.TEMPLATECATEGORY TC' +
--     --                 ' ON PC.CATEGORYID = TC.CATEGORYID' +
--     --                 ' WHERE PC.URNUMBER = P.URNUMBER) AS [CATEGORY], ' +

--     --                 '(SELECT TOP 1 PROCEDUREDATE FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER = P.URNUMBER) AS [PROCEDURE],' +
--     --                 ' FROM DDDM_TPS_1.DBO.PATIENT P WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')''';
--     SET @SELECTQUERY = '''SELECT URNumber, FirstName FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')'

--     DECLARE @COMMAND NVARCHAR(MAX);
--     SET @COMMAND = @INSERTQUERY + ' FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY + ''');'

--     -- DECLARE @testQUERY NVARCHAR(MAX);
--     -- SET @testQUERY =  'INSERT INTO TEMPTABLE SELECT * FROM OPENROWSET(''SQLNCLI'', ' + 
--     --                     '''' + @CONNECTIONSTRING + ''',' +
--     --                     '''SELECT URNumber, FirstName FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')'');'

-- -- PRINT('---- this is the command ----  ' + @testQUERY);

--   SELECT *
--     FROM TEMPTABLE
    
--     PRINT('---- this is the command ----  ' + @COMMAND);
--     EXECUTE(@COMMAND);

--     SELECT *
--     FROM TEMPTABLE

-- END;


-- exec DIM_PATIENT_TRANSFER_GOOD;



