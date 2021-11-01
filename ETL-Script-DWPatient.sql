-- Student ID: 103200214
-- Name: Lachlan Taylor
-- 08/09/2021
-- github repo https://github.com/LTaylor137/NHDW-DDL



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

-- do the above for Patient
-- Measurement
-- Datapoint



-------------------------------------------------------------------------------------------
---------------------------   table lookups for testing    --------------------------------
-------------------------------------------------------------------------------------------



-- SELECT *
-- FROM NHDW_LDT_0214.DBO.DW_PATIENT

-- SELECT *
-- FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT

-- SELECT *
-- FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD

-- SELECT *
-- FROM NHDW_LDT_0214.DBO.ERROR_EVENT



--------------------------------------------------------------------------------
-------------------------------- INSTRUCTIONS ----------------------------------
--------------------------------------------------------------------------------



-- This script should run from top to bottom, creating all procedures in order of reference needs
-- at the end of this file the script will EXECUTE ETL_PROCEDURE_DWPATIENT, inserting appropriate data

-- please run this ETL scripts for ETL-Script-DWPatient, ETL-Script-DWMeasurement, 
-- and ETL-Script-DWDataPointRecord, then EXECUTE THE_ONE_QUERY in the file ETL-Script-CRON JOB



--------------------------------------------------------------------------------
---------------- DROP FUNCTIONS TO ALLOW RUN WHOLE SCRIPT AT ONCE --------------
--------------------------------------------------------------------------------



USE NHDW_LDT_0214;



DROP PROCEDURE IF EXISTS RUN_PATIENT_FILTERS;
GO
DROP PROCEDURE IF EXISTS RUN_PATIENT_MODIFY;
GO
DROP PROCEDURE IF EXISTS TRANSFER_GOOD_DATA_INTO_DW_PATIENT;
GO
DROP FUNCTION IF EXISTS GET_CONNECTION_STRING;
GO
DROP TYPE IF EXISTS TEMP_PATIENT_TABLE_TYPE;
GO
DROP PROCEDURE IF EXISTS ETL_PROCEDURE_DWPATIENT;
GO
DROP TYPE IF EXISTS INCORRECT_GENDER_URNUMBERS_TYPE;
GO



--------------------------------------------------------------------------------
-------------------- CREATE GET CONNECTION STRING FUNCTION  --------------------
--------------------------------------------------------------------------------



-- CREATE GET CONNECTION STRING FUNCTION.
USE NHDW_LDT_0214;

-- DROP FUNCTION IF EXISTS GET_CONNECTION_STRING;
GO
CREATE FUNCTION GET_CONNECTION_STRING() RETURNS NVARCHAR(MAX) AS
BEGIN
    RETURN 'Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;';
END;
GO



--------------------------------------------------------------------------------
------------------------- Create a temporary table type ------------------------
--------------------------------------------------------------------------------



-- DROP TYPE IF EXISTS TEMP_PATIENT_TABLE_TYPE;
GO

CREATE TYPE TEMP_PATIENT_TABLE_TYPE AS TABLE (
    URNUMBER NVARCHAR(10),
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
);



-- craete table type to hold genders temporarily
DROP TYPE IF EXISTS INCORRECT_GENDER_URNUMBERS_TYPE;
GO

CREATE TYPE INCORRECT_GENDER_URNUMBERS_TYPE AS TABLE (
    URNUMBER NVARCHAR(10)
);



----------------------------------------------------------------------------------------
----------------------------------- Apply Filters --------------------------------------
---------------------- Problem 4 apply any filters to the data -------------------------
----------------------------------------------------------------------------------------



-- DROP PROCEDURE IF EXISTS RUN_PATIENT_FILTERS
GO

CREATE PROCEDURE RUN_PATIENT_FILTERS
    @DATA TEMP_PATIENT_TABLE_TYPE READONLY
AS
BEGIN
    BEGIN TRY

            -- Gender != Male or FEMALE
            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.URNUMBER, 'NHRM', 'Patient', 'P1', SYSDATETIME(), 'MODIFY'
    FROM @DATA D
    WHERE D.GENDER NOT IN ('MALE', 'FEMALE');

            -- DOB != 4
            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.URNUMBER, 'NHRM', 'Patient', 'P2', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE LEN(D.DOB) != 4;

            -- POSTCODE != 4
            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.URNUMBER, 'NHRM', 'Patient', 'P3', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE LEN(D.POSTCODE) != 4;

            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.URNUMBER, 'NHRM', 'Patient', 'P4', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.DIAGNOSIS IS NULL;

            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.URNUMBER, 'NHRM', 'Patient', 'P5', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.CATEGORY IS NULL;

            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.URNUMBER, 'NHRM', 'Patient', 'P6', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.[PROCEDURE] IS NULL;

    END TRY 

    BEGIN CATCH
        BEGIN
        DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
        THROW 50000, @ERROR, 1
    END
    END CATCH

END



----------------------------------------------------------------------------------------------------
---------------------------------------------- Modify ----------------------------------------------
---------- Problem 6 insert any data which the filter rules say needs to be transformed ------------
----------------------------------------------------------------------------------------------------



-- -- Modify wrong genders.
-- DROP PROCEDURE IF EXISTS RUN_PATIENT_MODIFY
GO

CREATE PROCEDURE RUN_PATIENT_MODIFY
    @DATA TEMP_PATIENT_TABLE_TYPE READONLY
AS
BEGIN

    DECLARE @INCORRECT_GENDER_URNUMBERS AS INCORRECT_GENDER_URNUMBERS_TYPE
    INSERT INTO @INCORRECT_GENDER_URNUMBERS
    SELECT EE.SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT EE
    WHERE EE.FILTERID = 'P1'
        AND EE.[ACTION] = 'MODIFY'

    -- SELECT 'wrong gender' AS WRONGGENDER, *
    -- FROM @INCORRECT_GENDER_URNUMBERS;

    INSERT INTO NHDW_LDT_0214.DBO.DW_PATIENT
        (
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
        CATEGORYNAME,
        PROCEDUREDATE,
        DIAGNOSIS)
    SELECT
        D.URNUMBER,
        'NRHM',
        'Patient',
        (SELECT GS.NEW_VALUE
        FROM NHDW_LDT_0214.DBO.GENDERSPELLING GS
        WHERE D.GENDER = GS.INVALID_VALUE),
        D.DOB,
        D.SUBURB,
        D.POSTCODE,
        D.COUNTRYOFBIRTH,
        D.LIVESALONE,
        D.ACTIVE,
        D.CATEGORY,
        D.[PROCEDURE],
        DIAGNOSIS
    FROM @DATA D
    WHERE D.URNUMBER IN (SELECT URNUMBER
    FROM @INCORRECT_GENDER_URNUMBERS);

    -- SELECT 'EE State A', *
    -- FROM NHDW_LDT_0214.DBO.ERROR_EVENT

    DELETE FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    WHERE SOURCE_ID IN (SELECT URNUMBER
    FROM @INCORRECT_GENDER_URNUMBERS);

    -- SELECT 'EE State B', *
    -- FROM NHDW_LDT_0214.DBO.ERROR_EVENT

END



----------------------------------------------------------------------------------------
------------------------ Transfer good data into DW PATIENT ----------------------------
-------------------------- Problem 5 insert the good data ------------------------------
----------------------------------------------------------------------------------------



-- DROP PROCEDURE IF EXISTS TRANSFER_GOOD_DATA_INTO_DW_PATIENT
GO

CREATE PROCEDURE TRANSFER_GOOD_DATA_INTO_DW_PATIENT
    @DATA TEMP_PATIENT_TABLE_TYPE READONLY
AS

BEGIN

    BEGIN TRY

        INSERT INTO NHDW_LDT_0214.DBO.DW_PATIENT
            (
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
            CATEGORYNAME,
            PROCEDUREDATE,
            DIAGNOSIS)
        SELECT
            D.URNUMBER,
            'NRHM',
            'Patient',
            D.GENDER,
            D.DOB,
            D.SUBURB,
            D.POSTCODE,
            D.COUNTRYOFBIRTH,
            D.LIVESALONE,
            D.ACTIVE,
            D.CATEGORY,
            D.[PROCEDURE],
            D.DIAGNOSIS
        FROM @DATA D
        WHERE D.URNUMBER NOT IN (SELECT SOURCE_ID
        FROM NHDW_LDT_0214.DBO.ERROR_EVENT)
        AND D.URNUMBER NOT IN (SELECT URNUMBER 
        FROM NHDW_LDT_0214.DBO.DW_PATIENT);

    END TRY

    BEGIN CATCH
        BEGIN
            DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
            THROW 50000, @ERROR, 1
        END
    END CATCH

END;



--------------------------------------------------------------------------------
------------------------------ Select Procedure ------------------------------
--------------------------------------------------------------------------------



-- DROP PROCEDURE IF EXISTS ETL_PROCEDURE_DWPATIENT
GO

CREATE PROCEDURE ETL_PROCEDURE_DWPATIENT
AS
BEGIN

    PRINT '--- ETL_PROCEDURE_DWPATIENT has begun ---'

    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
    FROM NHDW_LDT_0214.DBO.DW_PATIENT
    IF (@ALREADY_IN_DIM IS NULL)
        SET @ALREADY_IN_DIM = '0'

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    IF (@IN_ERROR_EVENT IS NULL)
        SET @IN_ERROR_EVENT = '0'

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;
    PRINT 'List of IDs to exclude: ' + CHAR(13)+CHAR(10) + @TO_EXCLUDE;

    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

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

    DECLARE @COMMAND_P NVARCHAR(MAX);
    SET @COMMAND_P = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY + ');'
    PRINT('--- This is the command string: ' + @COMMAND_P);

    DECLARE @TEMPPATIENTTABLE AS TEMP_PATIENT_TABLE_TYPE;

    -- SELECT 'PTT State A', *
    -- FROM @TEMPPATIENTTABLE;

    INSERT INTO @TEMPPATIENTTABLE
    EXECUTE(@COMMAND_P);

    -- -- inserting test data to spoof gender filters.
    -- INSERT INTO @TEMPPATIENTTABLE
    -- VALUES
    --     ('123450001', 'Fem', 1932, 'Springfield', 1234, 'Australia', 0, 1, 'xxx', 'Indwelling Pleural Catheter', 'Oct 13 2020 12:00AM'),
    --     ('123450002', 'F', 1932, 'Springfield', 1234, 'Australia', 0, 1, 'xxx', 'Indwelling Pleural Catheter', 'Oct 13 2020 12:00AM'),
    --     ('123450003', 'Mail', 1932, 'Springfield', 1234, 'Australia', 0, 1, 'xxx', 'Indwelling Pleural Catheter', 'Oct 13 2020 12:00AM')

    -- SELECT 'PTT State B', *
    -- FROM @TEMPPATIENTTABLE;

    EXEC RUN_PATIENT_FILTERS @DATA = @TEMPPATIENTTABLE;

    EXEC RUN_PATIENT_MODIFY @DATA = @TEMPPATIENTTABLE;

    EXEC TRANSFER_GOOD_DATA_INTO_DW_PATIENT @DATA = @TEMPPATIENTTABLE

    PRINT '--- ETL_PROCEDURE_DWPATIENT has finished ---'

END;



-------------------------------------------------------------------------------------------
------------------------- EXECUTE ETL_PROCEDURE_DWDATAPOINTRECORD -------------------------
-------------------------------------------------------------------------------------------



GO
EXEC ETL_PROCEDURE_DWPATIENT;




-------------------------------------------------------------------------------------------
---------------------------   table lookups for testing    --------------------------------
-------------------------------------------------------------------------------------------



-- SELECT *
-- FROM NHDW_LDT_0214.DBO.DW_PATIENT

-- SELECT *
-- FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT

-- SELECT *
-- FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD

-- SELECT *
-- FROM NHDW_LDT_0214.DBO.ERROR_EVENT




--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------    The below contains workings out for future reference     -----------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
----------------------------- General table lookups ----------------------------
--------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------
------------------------------------ insert test data -------------------------------------
-------------------------------------------------------------------------------------------




-- USE NHDW_LDT_0214;

-- INSERT INTO ERROR_EVENT
-- VALUES
--     (900000005, 'NHRM', 'TABLE', 1, GETDATE(), 'SKIP'),
--     (900000010, 'NHRM', 'TABLE', 1, GETDATE(), 'SKIP')

-- INSERT INTO DW_PATIENT
-- VALUES
--     (900000015, 'NHRM', 'TABLE', 'MALE', 1980, 'Suburb', '3000', 'Australia', '0', '1', 'IPC', '12-12-2020', 'Bad'),
--     (900000020, 'NHRM', 'TABLE', 'FEMALE', 1980, 'Suburb', '3000', 'Australia', '0', '1', 'IPC', '12-12-2020', 'Bad')

-- go;







-- SELECT NAME FROM SYS.DATABASES;

-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- SELECT * FROM master.sys.sql_logins;

-- SELECT * FROM [DDDM_TPS_1].sys.sql_logins;

-- USE DDDM_TPS_1

-- SELECT * FROM sys.objects

-- SELECT *
-- FROM measurementrecord
-- SELECT *
-- FROM patientmeasurement
-- SELECT *
-- FROM datapointrecord
-- SELECT *
-- FROM measurement

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

-- db.cgau35jk6tdb.us-east-1.rds.amazonaws.com
-- Database:  DDDM_TPS_1
-- admin
-- Kitemud$41

-- the below is needed to be run on the master level of the source database server 
-- so that people can login remotely.

-- use master;
-- GO

-- EXEC sp_configure 'show advanced options', 1;
-- RECONFIGURE;
-- GO

-- exec sp_configure 'Ad Hoc Distributed Queries', 1;
-- RECONFIGURE;  
-- GO

-- EXEC sp_configure 'xp_cmdshell', 1
-- RECONFIGURE
-- GO

-- Drop user ldtjobmanager;

-- Drop login ldtjobmanager;

-- -- first run this

-- use master;
-- GO

-- CREATE LOGIN ldtreadonly WITH PASSWORD = 'Kitemud$41';
-- GO

-- -- then run this.

-- USE [DDDM_TPS_1];
-- GO

-- CREATE USER ldtreadonly FOR LOGIN ldtreadonly;
-- GO

-- EXEC sp_addrolemember [db_datareader], ldtreadonly;

-- note sp_addrolemember is a "stored proceedure"


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
----------------- Problem 1 Piecing together our query to exclude data already in the DW and EE. -----------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------


-- get list of all patients not required -- patients already in dw -- patients in EE 
-- e.g. "(900000015,900000020,900000005,900000010)"

-- USE NHDW_LDT_0214

-- DROP PROCEDURE IF EXISTS GET_IDS_TO_EXCLUDE
-- GO
-- CREATE PROCEDURE GET_IDS_TO_EXCLUDE
-- AS
-- BEGIN

--     -- GET THE 
--     DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
--     SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
--     FROM NHDW_LDT_0214.DBO.DW_PATIENT
--     -- WHERE DWSOURCEDB = 'NHDW_LDT_0214';
--     -- PRINT @ALREADY_IN_DIM;

--     DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
--     SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
--     FROM NHDW_LDT_0214.DBO.ERROR_EVENT
--     -- WHERE DWSOURCEDB = 'NHDW_LDT_0214';
--     -- PRINT @IN_ERROR_EVENT;

--     -- resolves issue if @ALREADY_IN_DIM contained no values, then @TO_EXCLUDE would not SET at all.
--     IF (@ALREADY_IN_DIM IS NULL)
--         SET @ALREADY_IN_DIM = '0'

--     IF (@IN_ERROR_EVENT IS NULL)
--         SET @IN_ERROR_EVENT = '0'

--     DECLARE @TO_EXCLUDE NVARCHAR(MAX)
--     SET @TO_EXCLUDE = '(' + @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT + ')';

--     print 'List of IDs to exclude ' + CHAR(13)+CHAR(10) + @TO_EXCLUDE;

-- -- DECLARE @COMMAND2 NVARCHAR(MAX);
-- -- SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
-- --                 '''Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
-- --                 '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN ''' + @EXISTING_PATIENT_IDS + ''');'        
-- -- EXEC(@COMMAND2);
-- END;

-- EXEC GET_IDS_TO_EXCLUDE;




--------------------------------------------------------------------------------
------------------------ Tims Solutions and examples ---------------------------
--------------------------------------------------------------------------------



-- execute the below while logged into your own DW server.
-- OPENROWSET can only accept strings, not variables.

-- SELECT *
-- FROM
--     OPENROWSET('SQLNCLI', 'Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
-- 'SELECT * FROM DDDM_TPS_1.dbo.PATIENT');

-- SELECT *
-- FROM
--     OPENROWSET('SQLNCLI', 'Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
-- 'SELECT * FROM DDDM_TPS_1.dbo.PATIENT
-- WHERE URNUMBER NOT IN (SELECT SOURCEID FROM DW_PATIENT)');

-- -- EXAMPLE OF CREATE A COMMAND STRING.

-- BEGIN
--     DECLARE @COMMAND1 NVARCHAR(MAX);

--     SET @COMMAND1 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
--                     '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT'');'
--     -- PRINT(@COMMAND1);          
--     EXEC(@COMMAND1);
-- END

-- -- EXAMPLE CREATE A COMMAND STRING WITH WHERE LOGIC.

-- BEGIN
--     DECLARE @COMMAND2 NVARCHAR(MAX);
--     SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
--                     '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (900000001, 900000002)'');'
--     PRINT('---- this is the command ----  ' + @COMMAND2);
--     EXEC(@COMMAND2);
-- END

-- GO


-- -- EXAMPLE CREATE A COMMAND STRING USING ROWNUMS.

-- -- GO AND GET A LIST OF ID'S THAT EXIST IN THE ERROR EVENT TABLE, AND THE DW_PATIENT TABLE.
-- BEGIN

--     -- GO AND GET THE CONNECTION STRING.
--     DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
--     EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

--     -- SET UP A STRING OF ID NUMBERS TO EXCLUDE FROM COMMAND (i.e. numbers already existing in EE or DW.)
--     DECLARE @ROWNUMS NVARCHAR(MAX);
--     SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
--     FROM NHDW_LDT_0214.DBO.LIST_OF_ROWNUMS
--     PRINT (@ROWNUMS);

--     --CREATE THE COMMAND TO SEND TO THE OTHER SERVER
--     DECLARE @COMMAND NVARCHAR(MAX);
--     SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''' + @CONNECTIONSTRING + ''',' +
--                     '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
--     PRINT(@COMMAND);
--     EXEC(@COMMAND);

-- END


-- ------------------ TEMPORARY TABLE METHOD ------------------

-- USE NHDW_LDT_0214;

-- GO

-- -- Using a temporary table method.

-- -- THIS PROCEDURE GETS THE DATA AND PLACES IT INTO XXX  VERSION 2
-- DROP PROCEDURE IF EXISTS GETTEMPDATA;
-- GO
-- CREATE PROCEDURE GETTEMPDATA
-- AS
-- BEGIN

--     DROP TABLE IF EXISTS TEMPTABLE1;
--     CREATE TABLE TEMPTABLE1
--     (
--         TESTID INT,
--         TESTDATA NVARCHAR(100)
--     )

--     -- CREATE CONNECTION STRING
--     DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
--     EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

--     DECLARE @COMMAND NVARCHAR(MAX);
--     SET @COMMAND = 'INSERT INTO TEMPTABLE SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''' + @CONNECTIONSTRING + ''',' +
--                     '''SELECT URNumber, SurName FROM DDDM_TPS_1.dbo.PATIENT'');'

--     PRINT 'BEFORE'

--     SELECT *
--     FROM TEMPTABLE1

--     EXEC(@COMMAND);

--     PRINT 'AFTER'

--     SELECT *
--     FROM TEMPTABLE1

-- END;

-- GO

-- DROP PROCEDURE IF EXISTS VAR_SELECT_TEST;
-- GO
-- CREATE PROCEDURE VAR_SELECT_TEST
-- AS
-- BEGIN
--     EXEC GETTEMPDATA;
-- END;

-- -- EXECUTE AFTER INITIALISING PROCEDURES
-- EXEC VAR_SELECT_TEST;




-- -------------------------Using a variable datatype method.---------------------------------




--- Create a temporary table type ---

-- DROP TYPE IF EXISTS TEMP_PATIENT_TABLE_TYPE;
-- GO
-- CREATE TYPE TEMP_PATIENT_TABLE_TYPE AS TABLE (
--     URNUMBER INT,
--     GENDER NVARCHAR(10),
--     DOB INT,
--     SUBURB NVARCHAR(MAX),
--     POSTCODE NVARCHAR(4),
--     COUNTRYOFBIRTH NVARCHAR(MAX),
--     LIVESALONE NVARCHAR(1),
--     ACTIVE NVARCHAR(1),
--     [DIAGNOSIS] NVARCHAR(MAX),
--     [CATEGORY] NVARCHAR(MAX),
--     [PROCEDURE] NVARCHAR(MAX)
-- );

-- GO;

--- pull data from DDDM_TPS_1.DBO.PATIENT and insert it into a temptable.  ---

-- DROP PROCEDURE IF EXISTS TRANSFER_DATA_TO_TEMP_PATIENT_STORAGE
-- GO
-- CREATE PROCEDURE TRANSFER_DATA_TO_TEMP_PATIENT_STORAGE
-- AS
-- BEGIN

--     -- get a string of id's already in EE and DW tables.
--     DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
--     SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + URNUMBER
--     FROM NHDW_LDT_0214.DBO.DW_PATIENT
--     -- WHERE DWSOURCEDB = 'NHRM';

--     DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
--     SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
--     FROM NHDW_LDT_0214.DBO.ERROR_EVENT
--     -- WHERE DWSOURCEDB = 'NHRM';

--     DECLARE @TO_EXCLUDE NVARCHAR(MAX)
--     SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;
--     PRINT @TO_EXCLUDE;

--     -- get connection string
--     DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
--     EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

--     -- DROP TABLE IF EXISTS TEMPTABLE;
--     -- CREATE TABLE TEMPTABLE
--     -- (
--     --     URNUMBER INT,
--     --     GENDER NVARCHAR(10),
--     --     DOB INT,
--     --     SUBURB NVARCHAR(MAX),
--     --     POSTCODE NVARCHAR(4),
--     --     COUNTRYOFBIRTH NVARCHAR(MAX),
--     --     LIVESALONE NVARCHAR(1),
--     --     ACTIVE NVARCHAR(1),
--     --     [DIAGNOSIS] NVARCHAR(MAX),
--     --     [CATEGORY] NVARCHAR(MAX),
--     --     [PROCEDURE] NVARCHAR(MAX)
--     -- )

--     -- DECLARE @INSERTQUERY NVARCHAR(MAX);
--     -- SET @INSERTQUERY = 'INSERT INTO @TEMPPATIENTTABLE (URNUMBER, GENDER, DOB, SUBURB, POSTCODE, COUNTRYOFBIRTH, LIVESALONE, ACTIVE, [DIAGNOSIS], [CATEGORY], [PROCEDURE])'

--     -- write the code to get the required data - excludes those identified above.
--     DECLARE @SELECTQUERY NVARCHAR(MAX);
--     SET @SELECTQUERY = '''SELECT URNUMBER, GENDER, YEAR(DOB) AS DOB,' +
--                     'SUBURB, POSTCODE, COUNTRYOFBIRTH, LIVESALONE, ACTIVE, ' +
--                     '(SELECT TOP 1 DIAGNOSIS FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER = P.URNUMBER) AS [DIAGNOSIS],' +

--                     '(SELECT TOP 1 CATEGORYNAME FROM DDDM_TPS_1.DBO.PATIENTCATEGORY PC' +
--                     ' INNER JOIN DDDM_TPS_1.DBO.TEMPLATECATEGORY TC' +
--                     ' ON PC.CATEGORYID = TC.CATEGORYID' +
--                     ' WHERE PC.URNUMBER = P.URNUMBER) AS [CATEGORY], ' +

--                     '(SELECT TOP 1 PROCEDUREDATE FROM DDDM_TPS_1.DBO.CONDITIONDETAILS CD WHERE CD.URNUMBER = P.URNUMBER) AS [PROCEDURE]' +
--                     ' FROM DDDM_TPS_1.DBO.PATIENT P WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')''';

--     DECLARE @COMMAND NVARCHAR(MAX);
--     -- SET @COMMAND = @INSERTQUERY + ' FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY + ');'
--     SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY + ');'

--     PRINT('---- this is the command:   ' + @COMMAND);

--     DECLARE @TEMPPATIENTTABLE AS TEMP_PATIENT_TABLE_TYPE;
--     INSERT INTO @TEMPPATIENTTABLE
--     EXECUTE(@COMMAND);

--     SELECT *
--     FROM @TEMPPATIENTTABLE;

-- END;

-- EXEC TRANSFER_DATA_TO_TEMP_PATIENT_STORAGE;


-----------------------------


--     DROP TABLE IF EXISTS TEMPTABLE;
-- go
-- DECLARE @TEMPTABLEVAR TEMPTABLETYPE;
-- -- THINK OF THIS AS A TEMPLATE.
-- SELECT *
-- FROM @TEMPTABLEVAR;
-- -- CHECK CONTENTS OF TABLE BEFORE.

-- -- CREATE CONNECTION STRING
-- DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
-- EXEC @CONNECTIONSTRING = GET_CONNECTION_STRING;

-- -- SET UP A STRING OF ID NUMBERS TO EXCLUDE FROM COMMAND (i.e. numbers already existing in EE or DW.)
-- -- DECLARE @ROWNUMS NVARCHAR(MAX);
-- -- SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
-- -- FROM NHDW_LDT_0214.DBO.LIST_OF_ROWNUMS
-- -- PRINT (@ROWNUMS);

-- DECLARE @COMMAND NVARCHAR(MAX);
-- SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                     '''' + @CONNECTIONSTRING + ''',' +
--                     -- '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN (' + @ROWNUMS + ')'');'
--                     -- PULLS BASIC DATA 
--                     '''SELECT URNumber, FirstName FROM DDDM_TPS_1.dbo.PATIENT'');'

-- -- PRINT(@COMMAND);
-- INSERT INTO @TEMPTABLEVAR
-- EXEC(@COMMAND);

-- EXEC TABLE_PARAM_TEST @IN_TABLE = @TEMPTABLEVAR;
-- -- E.G. ETL_NHRM_PATIENT_FILTER_1

-- SELECT *
-- FROM @TEMPTABLEVAR;
-- -- CHECK CONTENTS OF TABLE AFTER.

-- END;

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




--         -- Postcode is not 4 numbers long.
--         DROP PROCEDURE IF EXISTS FILTER_2_DOB
--         GO
--         CREATE PROCEDURE FILTER_2_DOB
--         AS
--         BEGIN
--             BEGIN TRY
--                 BEGIN TRAN

--                     INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
--                 (SOURCE_ID, SOURCE_DATABASE,
--                 SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
--             SELECT TT.URNUMBER, 'NHRM', 'TABLE', '1', SYSDATETIME(), 'SKIP'
--             FROM NHDW_LDT_0214.DBO.TEMPTABLE TT
--             WHERE LEN(TT.DOB) != 4;

--                     DELETE 
--                     FROM NHDW_LDT_0214.DBO.TEMPTABLE
--                     WHERE LEN(DOB) != 4;

--                 COMMIT TRAN

--             END TRY 

--             BEGIN CATCH

--             -- catch errors here.

--             END CATCH

--         END

--         -- Postcode is not 4 numbers long.
--         DROP PROCEDURE IF EXISTS FILTER_3_POSTCODE
--         GO
--         CREATE PROCEDURE FILTER_3_POSTCODE
--         AS
--         BEGIN
--             BEGIN TRY
--                 BEGIN TRAN

--                     INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
--                 (SOURCE_ID, SOURCE_DATABASE,
--                 SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
--             SELECT TT.URNUMBER, 'NHRM', 'TABLE', '1', SYSDATETIME(), 'SKIP'
--             FROM NHDW_LDT_0214.DBO.TEMPTABLE TT
--             WHERE LEN(TT.POSTCODE) != 4;

--                     DELETE 
--                     FROM NHDW_LDT_0214.DBO.TEMPTABLE
--                     WHERE LEN(POSTCODE) != 4;

--                 COMMIT TRAN

--             END TRY 

--             BEGIN CATCH

--             -- catch errors here.

--             END CATCH

--         END

--         -- Category is null - 
--         DROP PROCEDURE IF EXISTS FILTER_4_CATEGORY
--         GO
--         CREATE PROCEDURE FILTER_4_CATEGORY
--         AS
--         BEGIN
--             BEGIN TRY
--                 BEGIN TRAN

--                     INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
--                 (SOURCE_ID, SOURCE_DATABASE,
--                 SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
--             SELECT TT.URNUMBER, 'NHRM', 'TABLE', '1', SYSDATETIME(), 'SKIP'
--             FROM NHDW_LDT_0214.DBO.TEMPTABLE TT
--             WHERE TT.CATEGORY IS NULL;

--                     DELETE 
--                     FROM NHDW_LDT_0214.DBO.TEMPTABLE
--                     WHERE CATEGORY IS NULL;

--                 COMMIT TRAN

--             END TRY 

--             BEGIN CATCH

--             -- catch errors here.

--             END CATCH

--         END


-- END

