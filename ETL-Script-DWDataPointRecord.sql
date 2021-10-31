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

-- SELECT * FROM sys.objects

SELECT *
FROM measurementrecord
SELECT *
FROM patientmeasurement
SELECT *
FROM datapointrecord
SELECT *
FROM measurement

USE NHDW_LDT_0214;

USE DDDM_TPS_1;


USE DDDM_TPS_1
SELECT *
FROM measurementrecord
SELECT *
FROM patientmeasurement
SELECT *
FROM datapointrecord
SELECT *
FROM measurement
SELECT *
FROM datapoint


-- select works correctly from within source DB.
SELECT MR.URNumber, MR.MeasurementRecordID, CONVERT(CHAR(8), MR.DateTimeRecorded, 112) AS DWDATETIMEKEY,
    DPR.[VALUE], CONVERT(CHAR(8), PM.FrequencySetDate, 112) AS FREQUENCYSETDATE, Frequency
FROM DDDM_TPS_1.dbo.measurementrecord MR
    INNER JOIN DDDM_TPS_1.DBO.PATIENT P
    ON MR.URNumber = P.URNUMBER
    INNER JOIN DDDM_TPS_1.dbo.datapointrecord DPR
    ON MR.MeasurementRecordID = DPR.MeasurementRecordID
    INNER JOIN DDDM_TPS_1.dbo.patientmeasurement PM
    ON MR.URNumber = PM.URNUMBER



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
----------------- Problem 1 Piecing together our query to exclude data already in the DW and EE. -----------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

DW_PATIENT
(
    DWPATIENTID INTEGER IDENTITY(1,1),
    URNUMBER NVARCHAR(50) NOT NULL,

CREATE TABLE DW_MEASUREMENT
(
    DWMEASUREMENTID INTEGER IDENTITY(1,1),
    MEASUREMENTRECORDID NVARCHAR(50) NOT NULL,

-- this selects ID's from source DW tables and concats them
SELECT P.URNUMBER, P.DWPATIENTID, M.DWMEASUREMENTID, M.MEASUREMENTRECORDID, DPR.DWDATEKEY,
concat(P.URNUMBER, '-', M.DWMEASUREMENTID,'-', DPR.DWDATEKEY)
FROM DW_PATIENT P 
INNER JOIN DW_DWDATAPOINTRECORD DPR
ON P.DWPATIENTID = DPR.DWPATIENTID
INNER JOIN DW_MEASUREMENT M
ON M.DWMEASUREMENTID = DPR.DWMEASUREMENTID


id to exclude should = 900000335-13-20200110

SRC_URNUMBER = 900000335
SRC_MEASUREMENTRECORDID = 13
DWDATEKEY = 20200110

--DW_DWDATAPOINTRECORD
dwpatient  dwmeasurenmtnID DWDATEKEY
215         2               20200110



SELECT *
FROM NHDW_LDT_0214.DBO.DW_PATIENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD

SELECT *
FROM NHDW_LDT_0214.DBO.ERROR_EVENT


--check the temptable.
EXEC ETL_PROCEDURE_DWDATAPOINTRECORD;




-- -- EXAMPLE CREATE A COMMAND STRING WITH WHERE LOGIC.

BEGIN
    DECLARE @COMMAND2 NVARCHAR(MAX);
    SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
                    '''Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
                    '''SELECT CONCAT(MR.URNumber, ''-'', MR.Title, ''-'', Surname), * FROM DDDM_TPS_1.dbo.PATIENT'');'
    PRINT('---- this is the command ----  ' + @COMMAND2);
    EXEC(@COMMAND2);
END

SELECT * FROM OPENROWSET('SQLNCLI', 'Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;','SELECT CONCAT(MR.URNumber, ''-'', MR.Title, ''-'', Surname), * FROM DDDM_TPS_1.dbo.PATIENT'');'


DROP TYPE IF EXISTS GET_DPR_ROWS_TO_EXCLUDE;
GO
CREATE TYPE GET_DPR_ROWS_TO_EXCLUDE AS TABLE (
    DWPATIENTID NVARCHAR(10),
    DWMEASUREMENTID NVARCHAR(10),
    DWDATEKEY NVARCHAR(10)
);


    SELECT
        DWP.DWPATIENTID,
        DWM.DWMEASUREMENTID,
        DWDD.DateKey,
    FROM @DATA D
        INNER JOIN NHDW_LDT_0214.DBO.DW_PATIENT DWP
        ON D.SRC_URNUMBER = DWP.URNUMBER
        INNER JOIN NHDW_LDT_0214.DBO.DW_MEASUREMENT DWM
        ON D.SRC_MEASUREMENTRECORDID = DWM.MEASUREMENTRECORDID
        INNER JOIN NHDW_LDT_0214.DBO.DW_DIM_DATE DWDD
        ON D.DWDATEKEY = DWDD.DateKey




DROP PROCEDURE IF EXISTS GET_IDS_TO_EXCLUDE_DPR
GO
CREATE PROCEDURE GET_IDS_TO_EXCLUDE_DPR
AS
BEGIN

    -- GET THE 
    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + DPRCONCATPK
    FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD
    -- WHERE DWSOURCEDB = 'NHDW_LDT_0214';
    -- PRINT @ALREADY_IN_DIM;

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    WHERE LEN(SOURCE_ID) > 10;
    -- PRINT @IN_ERROR_EVENT;

    -- resolves issue if @ALREADY_IN_DIM contained no values, then @TO_EXCLUDE would not SET at all.
    IF (@ALREADY_IN_DIM IS NULL)
        SET @ALREADY_IN_DIM = '0'

    IF (@IN_ERROR_EVENT IS NULL)
        SET @IN_ERROR_EVENT = '0'

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = '(' + @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT + ')';

    print 'List of IDs to exclude ' + CHAR(13)+CHAR(10) + @TO_EXCLUDE;

-- DECLARE @COMMAND2 NVARCHAR(MAX);
-- SET @COMMAND2 = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' +
--                 '''Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;'',' +
--                 '''SELECT * FROM DDDM_TPS_1.dbo.PATIENT WHERE URNUMBER NOT IN ''' + @EXISTING_PATIENT_IDS + ''');'        
-- EXEC(@COMMAND2);
END;

EXEC GET_IDS_TO_EXCLUDE_DPR;



------------------------------------------------------------------------------------------------------------------------
--------------------------------- Problem 2 Get the required data from the source. -------------------------------------
------------------------------------------------------------------------------------------------------------------------


-------------------

GO

-- CREATE GET CONNECTION STRING FUNCTION.
USE NHDW_LDT_0214;

DROP FUNCTION IF EXISTS GET_CONNECTION_STRING;
GO
CREATE FUNCTION GET_CONNECTION_STRING() RETURNS NVARCHAR(MAX) AS
BEGIN
    RETURN 'Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;';
END;
GO


--------------------------------------------------------------------------------
-------------------------------- Temp table type- ------------------------------
--------------------------------------------------------------------------------



-- create temp table type before procedure
DROP TYPE IF EXISTS TEMP_DATAPOINTRECORD_TABLE_TYPE;
GO
CREATE TYPE TEMP_DATAPOINTRECORD_TABLE_TYPE AS TABLE (
    SRC_URNUMBER NVARCHAR(50) NOT NULL,
    SRC_MEASUREMENTRECORDID NVARCHAR(50) NOT NULL,
    DWDATEKEY NVARCHAR(50) NOT NULL,
    SRC_VALUE FLOAT(10) NOT NULL,
    FREQUENCYDATEKEY INTEGER NOT NULL,
    FREQUENCY INTEGER NOT NULL
);

    -- DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    -- SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') +''''+ DPRCONCATPK +''''
    -- FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD
    -- -- WHERE DWSOURCEDB = 'NHDW_LDT_0214';
    -- PRINT(@ALREADY_IN_DIM);

--------------------------------------------------------------------------------
------------------------------ Transfer Procedure ------------------------------
--------------------------------------------------------------------------------


-- DROP FUNCTION IF EXISTS TEST
-- GO

-- DECLARE @TEST NVARCHAR(MAX);
--  SET @TEST = (SELECT TOP 1 CONCAT(URNumber, '-', MeasurementRecordID, 'hello')
--             FROM
--                 OPENROWSET('SQLNCLI', 'Server=db.cgau35jk6tdb.us-east-1.rds.amazonaws.com;UID=ldtreadonly;PWD=Kitemud$41;',
--             'SELECT URNumber, MeasurementRecordID FROM DDDM_TPS_1.dbo.measurementrecord')
--         );

-- PRINT @TEST; 


-- pull data from DDDM_TPS_1.DBO.PATIENT and insert it into a temptable. 
DROP PROCEDURE IF EXISTS ETL_PROCEDURE_DWDATAPOINTRECORD
GO
CREATE PROCEDURE ETL_PROCEDURE_DWDATAPOINTRECORD
AS
BEGIN
   
    DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + DPRCONCATPK 
    FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD
    -- PRINT @ALREADY_IN_DIM;

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE('' + @IN_ERROR_EVENT + ',', '' + '') + SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    WHERE LEN(SOURCE_ID) > 10;
    -- PRINT @IN_ERROR_EVENT;

    IF (@ALREADY_IN_DIM IS NULL)
        SET @ALREADY_IN_DIM = '0'

    IF (@IN_ERROR_EVENT IS NULL)
        SET @IN_ERROR_EVENT = '0'

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;

    print 'List of IDs to exclude ' + CHAR(13)+CHAR(10) + @TO_EXCLUDE;

    -- get connection string
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

    -- write the code to get the required data - excludes those identified above.
    DECLARE @SELECTQUERY03 NVARCHAR(MAX);
    SET @SELECTQUERY03 = 
        '''SELECT MR.URNumber, MR.MeasurementRecordID, ' +
        'CONVERT(CHAR(8), MR.DateTimeRecorded, 112) AS DWDATETIMEKEY, ' +
        'DPR.[VALUE], CONVERT(CHAR(8), PM.FrequencySetDate, 112) AS FREQUENCYSETDATE, Frequency ' +
        'FROM DDDM_TPS_1.dbo.measurementrecord MR ' +
        'INNER JOIN DDDM_TPS_1.DBO.PATIENT P ' +
        'ON MR.URNumber = P.URNUMBER ' +
        'INNER JOIN DDDM_TPS_1.dbo.datapointrecord DPR ' +
        'ON MR.MeasurementRecordID = DPR.MeasurementRecordID ' +
        'INNER JOIN DDDM_TPS_1.dbo.patientmeasurement PM ' +
        'ON MR.URNumber = PM.URNUMBER''';

    DECLARE @COMMAND_DPR NVARCHAR(MAX);

    IF @TO_EXCLUDE IS NULL
        BEGIN
         -- original that works.
            SET @COMMAND_DPR = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY03 + ') SOURCE;'
        END
    ELSE
        BEGIN
              SET @COMMAND_DPR = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY03 + ') SOURCE ' +
                                 'WHERE CONCAT(SOURCE.URNumber, ''-'', SOURCE.MeasurementRecordID, ''-'', SOURCE.DWDATETIMEKEY) NOT IN (' + @TO_EXCLUDE + ');'

        END

    PRINT('---- this is the command ----  ' + @COMMAND_DPR);

    DECLARE @TEMPDATAPOINTRECORDTABLE AS TEMP_DATAPOINTRECORD_TABLE_TYPE;

    SELECT 'TT DPR A' AS A, *
    FROM @TEMPDATAPOINTRECORDTABLE;

    INSERT INTO @TEMPDATAPOINTRECORDTABLE
    EXECUTE(@COMMAND_DPR);

    SELECT 'TT DPR B' AS B, *
    FROM @TEMPDATAPOINTRECORDTABLE;

    -- EXEC RUN_PATIENT_FILTERS @DATA = @TEMPDATAPOINTRECORDTABLE;

    EXEC TRANSFER_GOOD_DATA_INTO_DW_DATAPOINTRECORD @DATA = @TEMPDATAPOINTRECORDTABLE;

END;


-------------------------------------------------------------------------------------------
------------------------- EXECUTE ETL_PROCEDURE_DWDATAPOINTRECORD -------------------------
-------------------------------------------------------------------------------------------

EXEC ETL_PROCEDURE_DWDATAPOINTRECORD;

-------------------------------------------------------------------------------------------
------------------------- EXECUTE ETL_PROCEDURE_DWDATAPOINTRECORD -------------------------
-------------------------------------------------------------------------------------------



SELECT *
FROM NHDW_LDT_0214.DBO.DW_PATIENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD

SELECT *
FROM NHDW_LDT_0214.DBO.ERROR_EVENT




----------------------------------------------------------------------------------------
----------------------------------- Apply Filters --------------------------------------
----------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS RUN_PATIENT_FILTERS
GO

CREATE PROCEDURE RUN_PATIENT_FILTERS
    @DATA TEMP_DATAPOINTRECORD_TABLE_TYPE READONLY
AS
BEGIN
    BEGIN TRY

--   SRC_URNUMBER NVARCHAR(50) NOT NULL,
--     SRC_MEASUREMENTRECORDID NVARCHAR(50) NOT NULL,
--     DWDATEKEY INTEGER NOT NULL,
--     SCR_VALUE FLOAT(10) NOT NULL,
--     FREQUENCYDATEKEY INTEGER NOT NULL,
--     FREQUENCY INTEGER NOT NULL

            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT (SELECT concat(D.SRC_URNUMBER, '-', D.SRC_MEASUREMENTRECORDID,'-', D.DWDATEKEY)),
-- '123',
    -- FROM @DATA D
    --     INNER JOIN NHDW_LDT_0214.DBO.DW_PATIENT DWP
    --     ON D.SRC_URNUMBER = DWP.URNUMBER
    --     INNER JOIN NHDW_LDT_0214.DBO.DW_MEASUREMENT DWM
    --     ON D.SRC_MEASUREMENTRECORDID = DWM.MEASUREMENTRECORDID
    --     INNER JOIN NHDW_LDT_0214.DBO.DW_DIM_DATE DWDD
    --     ON D.DWDATEKEY = DWDD.DateKey),
     
     
     
     'NHRM', 'TABLE', 'DPR1', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.SRC_VALUE = '1'

   END TRY 

    BEGIN CATCH
        BEGIN
        DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
        THROW 50000, @ERROR, 1
    END
    END CATCH

END


----------------------------------------------------------------------------------------
------------------------------- Transfer into DW PATIENT -------------------------------
----------------------------------------------------------------------------------------
-- Problem 5 insert the good data

-- The idea is to transfer any remaining data to the patient table, 
-- after it has been modified, or removed.



DROP PROCEDURE IF EXISTS TRANSFER_GOOD_DATA_INTO_DW_DATAPOINTRECORD
GO
CREATE PROCEDURE TRANSFER_GOOD_DATA_INTO_DW_DATAPOINTRECORD
    @DATA TEMP_DATAPOINTRECORD_TABLE_TYPE READONLY
AS

BEGIN

    BEGIN TRY

        -- IF @DATA IS NOT NULL

        INSERT INTO NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD
        (
        DPRCONCATPK,
        DWPATIENTID,
        DWMEASUREMENTID,
        DWDATEKEY,
        [VALUE],
        FREQUENCYDATEKEY,
        FREQUENCY
        )
    SELECT


--     (SELECT P.URNUMBER, P.DWPATIENTID, M.DWMEASUREMENTID, M.MEASUREMENTRECORDID, DPR.DWDATEKEY,
-- concat(P.URNUMBER, '-', M.DWMEASUREMENTID,'-', DPR.DWDATEKEY)
-- FROM DW_PATIENT P 
-- INNER JOIN DW_DWDATAPOINTRECORD DPR
-- ON P.DWPATIENTID = DPR.DWPATIENTID
-- INNER JOIN DW_MEASUREMENT M
-- ON M.DWMEASUREMENTID = DPR.DWMEASUREMENTID),

concat(DWP.URNUMBER, '-', DWM.DWMEASUREMENTID,'-', DWDD.DateKey),

        DWP.DWPATIENTID,
        DWM.DWMEASUREMENTID,
        DWDD.DateKey,
        D.SRC_VALUE,
        D.FREQUENCYDATEKEY,
        D.FREQUENCY
    FROM @DATA D
        INNER JOIN NHDW_LDT_0214.DBO.DW_PATIENT DWP
        ON D.SRC_URNUMBER = DWP.URNUMBER
        INNER JOIN NHDW_LDT_0214.DBO.DW_MEASUREMENT DWM
        ON D.SRC_MEASUREMENTRECORDID = DWM.MEASUREMENTRECORDID
        INNER JOIN NHDW_LDT_0214.DBO.DW_DIM_DATE DWDD
        ON D.DWDATEKEY = DWDD.DateKey

    END TRY

    BEGIN CATCH
        BEGIN
        DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
        THROW 50000, @ERROR, 1
    END
    END CATCH

END;





