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

-- SELECT * FROM datapoint

-- USE NHDW_LDT_0214;


--------------------------------------------------------------------------------
------------------ CREATE GET CONNECTION STRING FUNCTION  ----------------------
--------------------------------------------------------------------------------


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

USE NHDW_LDT_0214;

-- create temp table type before procedure
DROP TYPE IF EXISTS TEMP_MEASUREMENT_TABLE_TYPE;
GO
CREATE TYPE TEMP_MEASUREMENT_TABLE_TYPE AS TABLE (
    MEASUREMENTRECORDID NVARCHAR(50) NOT NULL,
    MEASUREMENTID NVARCHAR(50) NOT NULL,
    DATAPOINTNUMBER NVARCHAR(50) NOT NULL,
    CATEGORYID NVARCHAR(50) NOT NULL,
    MEASUREMENTNAME NVARCHAR(50) NOT NULL,
    MEASUREMENTVALUE NVARCHAR(50) NOT NULL,
    UPPERLIMIT NVARCHAR(50) NOT NULL,
    LOWERLIMIT NVARCHAR(50) NOT NULL
);

--------------------------------------------------------------------------------
------------------------------ Transfer Procedure ------------------------------
--------------------------------------------------------------------------------



-- pull data from DDDM_TPS_1.DBO.PATIENT and insert it into a temptable. 
DROP PROCEDURE IF EXISTS ETL_PROCEDURE_DWMEASUREMENT
GO
CREATE PROCEDURE ETL_PROCEDURE_DWMEASUREMENT
AS
BEGIN

    -- -- get a string of id's already in EE and DW tables.
    -- DECLARE @ALREADY_IN_DIM NVARCHAR(MAX);
    -- SELECT @ALREADY_IN_DIM = COALESCE(@ALREADY_IN_DIM + ',', '') + MEASUREMENTRECORDID
    -- FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT
    -- -- WHERE DWSOURCEDB = 'NHRM';
    -- IF (@ALREADY_IN_DIM IS NULL)
    --     SET @ALREADY_IN_DIM = '0'

    DECLARE @IN_ERROR_EVENT NVARCHAR(MAX);
    SELECT @IN_ERROR_EVENT = COALESCE(@IN_ERROR_EVENT + ',', '') + SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    -- WHERE DWSOURCEDB = 'NHRM';
    IF (@IN_ERROR_EVENT IS NULL)
        SET @IN_ERROR_EVENT = '0'

    DECLARE @TO_EXCLUDE NVARCHAR(MAX)
    SET @TO_EXCLUDE = @IN_ERROR_EVENT;
    -- SET @TO_EXCLUDE = @ALREADY_IN_DIM + ',' + @IN_ERROR_EVENT;

    -- PRINT @TO_EXCLUDE;

    -- get connection string
    DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
    EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;

    -- write the code to get the required data - excludes those identified above.
    DECLARE @SELECTQUERY_MS NVARCHAR(MAX);
    SET @SELECTQUERY_MS = 
                    '''SELECT MR.MEASUREMENTRECORDID, ' +
                    'MR.MEASUREMENTID, DPR.DATAPOINTNUMBER, MR.CATEGORYID, DP.[NAME], ' + 
                    'DPR.VALUE, DP.LOWERLIMIT, DP.UPPERLIMIT ' + 
                    'FROM DDDM_TPS_1.dbo.measurementrecord MR ' + 
                    'INNER JOIN DDDM_TPS_1.dbo.datapointrecord DPR ' + 
                    'ON MR.MeasurementRecordID = DPR.MeasurementRecordID ' + 
                    'INNER JOIN DDDM_TPS_1.dbo.datapoint DP ' + 
                    'ON DP.MeasurementID = MR.MeasurementID ' +
                    'WHERE URNUMBER NOT IN (' + @TO_EXCLUDE + ')''';

    DECLARE @COMMAND_MS NVARCHAR(MAX);
    SET @COMMAND_MS = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY_MS + ');'

    PRINT('---- this is the command ----  ' + @COMMAND_MS);

    DECLARE @TEMPMEASUREMENTTABLE AS TEMP_MEASUREMENT_TABLE_TYPE;

    SELECT 'TT M A', *
    FROM @TEMPMEASUREMENTTABLE;

    INSERT INTO @TEMPMEASUREMENTTABLE
    EXECUTE(@COMMAND_MS);

 -- inserting test data to spoof gender filters.
    INSERT INTO @TEMPMEASUREMENTTABLE
    VALUES
        ('001', '3', '1', '3', 'Level of Pain', 0, 5, 1),
        ('002', '3', '1', '3', 'Level of Pain', 7, 5, 1)
    
    SELECT 'TT M B', *
    FROM @TEMPMEASUREMENTTABLE;


    EXEC RUN_MEASUREMENT_FILTERS @DATA = @TEMPMEASUREMENTTABLE;

    EXEC RUN_MEASUREMENT_MODIFY @DATA = @TEMPMEASUREMENTTABLE;

    EXEC TRANSFER_GOOD_DATA_INTO_DW_MEASUREMENT @DATA = @TEMPMEASUREMENTTABLE;

END;



-------------------------------------------------------------------------------------------
------------------------- EXECUTE ETL_PROCEDURE_DWMEASUREMENT -----------------------------
-------------------------------------------------------------------------------------------

EXEC ETL_PROCEDURE_DWMEASUREMENT;

-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------


SELECT *
FROM NHDW_LDT_0214.DBO.DW_PATIENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT

SELECT *
FROM NHDW_LDT_0214.DBO.ERROR_EVENT


----------------------------------------------------------------------------------------
----------------------------------- Apply Filters --------------------------------------
----------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS RUN_MEASUREMENT_FILTERS
GO

CREATE PROCEDURE RUN_MEASUREMENT_FILTERS
    @DATA TEMP_MEASUREMENT_TABLE_TYPE READONLY
AS

BEGIN

    BEGIN TRY

        -- UPPERLIMIT outside of range.
        INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.MEASUREMENTRECORDID, 'NHRM', 'TABLE', 'M1', SYSDATETIME(), 'MODIFY'
    FROM @DATA D
    WHERE D.MEASUREMENTID = 3 AND D.MEASUREMENTVALUE > (5);

            -- UPPERLIMIT outside of range.
        INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.MEASUREMENTRECORDID, 'NHRM', 'TABLE', 'M2', SYSDATETIME(), 'MODIFY'
    FROM @DATA D
    WHERE D.MEASUREMENTID = 3 AND D.MEASUREMENTVALUE < (1);

                INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.MEASUREMENTRECORDID, 'NHRM', 'TABLE', 'M3', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.DATAPOINTNUMBER IS NULL;

            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.MEASUREMENTRECORDID, 'NHRM', 'TABLE', 'M4', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.CATEGORYID IS NULL;

            INSERT INTO NHDW_LDT_0214.DBO.ERROR_EVENT
        (SOURCE_ID, SOURCE_DATABASE, SOURCE_TABLE, FILTERID, [DATETIME], [ACTION])
    SELECT D.MEASUREMENTRECORDID, 'NHRM', 'TABLE', 'M5', SYSDATETIME(), 'SKIP'
    FROM @DATA D
    WHERE D.[MEASUREMENTVALUE] IS NULL;

    END TRY

    BEGIN CATCH
        BEGIN
        DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
        THROW 50000, @ERROR, 1
    END
    END CATCH

END;




----------------------------------------------------------------------------------------------------
---------------------------------------------- Modify ----------------------------------------------
----------------------------------------------------------------------------------------------------
-- Problem 6 insert any data which the filter rules say needs to be transformed.



DROP PROCEDURE IF EXISTS RUN_MEASUREMENT_MODIFY
GO

CREATE PROCEDURE RUN_MEASUREMENT_MODIFY
    @DATA TEMP_MEASUREMENT_TABLE_TYPE READONLY
AS
BEGIN

    BEGIN TRY

        -- IF @DATA IS NOT NULL

-- If value is greater than 5, then just insert 5 instead.
        INSERT INTO NHDW_LDT_0214.DBO.DW_MEASUREMENT
        (MEASUREMENTRECORDID,
        DWSOURCEBD,
        DWSOURCETABLE,
        MEASUREMENTID,
        DATAPOINTNUMBER,
        CATEGORYID,
        MEASUREMENTNAME,
        MEASUREMENTVALUE,
        UPPERLIMIT,
        LOWERLIMIT)
    SELECT
        D.MEASUREMENTRECORDID,
        'DWSOURCEBD',
        'DWSOURCETABLE',
        D.MEASUREMENTID,
        D.DATAPOINTNUMBER,
        D.CATEGORYID,
        D.MEASUREMENTNAME,
        '5',
        D.UPPERLIMIT,
        D.LOWERLIMIT
    FROM @DATA D
    WHERE D.MEASUREMENTRECORDID IN (SELECT TOP 1 SOURCE_ID
        FROM NHDW_LDT_0214.DBO.ERROR_EVENT)
        AND (SELECT TOP 1 FILTERID
        FROM NHDW_LDT_0214.DBO.ERROR_EVENT)
        = 'M1'

    -- DELETE FROM NHDW_LDT_0214.DBO.ERROR_EVENT
    -- WHERE SOURCE_ID IN (SELECT D.MEASUREMENTRECORDID FROM @DATA D) 
    --     AND FILTERID = 'M1'

    END TRY

    BEGIN CATCH
        BEGIN
        DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
        THROW 50000, @ERROR, 1
    END
    END CATCH

END




----------------------------------------------------------------------------------------
----------------------- TRANSFER_GOOD_DATA_INTO_DW_MEASUREMENT -------------------------
----------------------------------------------------------------------------------------
-- Problem 5 insert the good data

go
USE NHDW_LDT_0214;

DROP PROCEDURE IF EXISTS TRANSFER_GOOD_DATA_INTO_DW_MEASUREMENT
GO
CREATE PROCEDURE TRANSFER_GOOD_DATA_INTO_DW_MEASUREMENT
    @DATA TEMP_MEASUREMENT_TABLE_TYPE READONLY
AS

BEGIN

    BEGIN TRY

        -- IF @DATA IS NOT NULL

    INSERT INTO NHDW_LDT_0214.DBO.DW_MEASUREMENT
        (MEASUREMENTRECORDID,
        DWSOURCEBD,
        DWSOURCETABLE,
        MEASUREMENTID,
        DATAPOINTNUMBER,
        CATEGORYID,
        MEASUREMENTNAME,
        MEASUREMENTVALUE,
        UPPERLIMIT,
        LOWERLIMIT)
    SELECT
        D.MEASUREMENTRECORDID,
        'DWSOURCEBD',
        'DWSOURCETABLE',
        D.MEASUREMENTID,
        D.DATAPOINTNUMBER,
        D.CATEGORYID,
        D.MEASUREMENTNAME,
        D.MEASUREMENTVALUE,
        D.UPPERLIMIT,
        D.LOWERLIMIT
    FROM @DATA D
    WHERE D.MEASUREMENTRECORDID NOT IN (SELECT SOURCE_ID
    FROM NHDW_LDT_0214.DBO.ERROR_EVENT);
    END TRY

    BEGIN CATCH
        BEGIN
        DECLARE @ERROR NVARCHAR(MAX) = ERROR_MESSAGE();
        THROW 50000, @ERROR, 1
    END
    END CATCH

END;






-- source DB table structures.

-- INSERT INTO PatientMeasurement
--     (MeasurementID,CategoryID,URNumber,Frequency,FrequencySetDate)
-- VALUES(1, 1, '123456789', 28, GETDATE()),
--     (2, 1, '123456789', 1, GETDATE()),
--     (3, 1, '123456789', 1, GETDATE()),
--     (4, 1, '123456789', 1, GETDATE()),
--     (5, 1, '123456789', 7, GETDATE()),
--     (1, 1, '987654321', 28, GETDATE()),
--     (2, 1, '987654321', 1, GETDATE()),
--     (3, 1, '987654321', 1, GETDATE()),
--     (4, 1, '987654321', 1, GETDATE()),
--     (5, 1, '987654321', 7, GETDATE());

--     INSERT INTO Measurement
--     (MeasurementName, Frequency)
-- VALUES('ECOG Status', 28),
--     ('Breathlessness', 1),
--     ('Level of Pain', 1),
--     ('Fluid Drain', 1),
--     ('Quality of Life', 7);

-- INSERT INTO DataPoint
--     (MeasurementID,DataPointNumber,UpperLimit,LowerLimit,[Name])
-- VALUES(1, 1, 4, 0, 'ECOG Status'),
--     (2, 1, 5, 1, 'Breathlessness'),
--     (3, 1, 5, 1, 'Level of Pain'),
--     (4, 1, 600, 0, 'Fluid Drain'),
--     (5, 1, 5, 1, 'Mobility'),
--     (5, 2, 5, 1, 'Self-Care'),
--     (5, 3, 5, 1, 'Usual-Activies'),
--     (5, 4, 5, 1, 'Pain/Discomfort'),
--     (5, 5, 5, 1, 'Anxiety/Depression'),
--     (5, 6, 100, 0, 'QoL Vas Health Slider');











    -- (
    --     DWMEASUREMENTID INTEGER NOT NULL,
    --     MEASUREMENTID INTEGER NOT NULL,
    --     DATAPOINTNUMBER INTEGER NOT NULL,
    --     DWSOURCEBD NVARCHAR(50) NOT NULL,
    --     DWSOURCETABLE NVARCHAR(50) NOT NULL,
    --     MEASUREMENTNAME NVARCHAR(50) NOT NULL,
    --     [NAME] NVARCHAR(50) NOT NULL,
    --     UPPERLIMIT INTEGER NOT NULL,
    --     LOWERLIMIT INTEGER NOT NULL,
    --     QUESTION NVARCHAR(255) NOT NULL,
    --     ANSWERNUMBER INTEGER NULL,
    --     PRIMARY KEY (DWMEASUREMENTID)
    -- );


-- CREATE TABLE DW_MEASUREMENT
--     pm DWMEASUREMENTID INTEGER NOT NULL, 
--     pm MEASUREMENTID INTEGER NOT NULL,
--     dp DATAPOINTNUMBER INTEGER NOT NULL,
--     x DWSOURCEBD NVARCHAR(50) NOT NULL,
--     x DWSOURCETABLE NVARCHAR(50) NOT NULL,
--     me MEASUREMENTNAME NVARCHAR(50) NOT NULL,
--     dp [NAME] NVARCHAR(50) NOT NULL,
--     dp UPPERLIMIT INTEGER NOT NULL,
--     dp LOWERLIMIT INTEGER NOT NULL,
--     QUESTION NVARCHAR(255) NOT NULL,
--     ANSWERNUMBER INTEGER NULL,






-- queries to use in Tims Source DB

-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- USE DDDM_TPS_1
-- SELECT *
-- FROM measurementrecord
-- SELECT *
-- FROM patientmeasurement
-- SELECT *
-- FROM datapointrecord
-- SELECT *
-- FROM measurement
-- SELECT *
-- FROM datapoint

-- SELECT
--     MR.MeasurementRecordID,
--     MR.DateTimeRecorded,
--     MR.URNumber,
--     MR.MeasurementID,
--     MR.CategoryID,
--     DPR.DATAPOINTNUMBER,
--     DPR.VALUE,
--     DP.[NAME],
--     DP.LOWERLIMIT,
--     DP.UPPERLIMIT
-- FROM measurementrecord MR
--     INNER JOIN datapointrecord DPR
--     ON MR.MeasurementRecordID = DPR.MeasurementRecordID
--     INNER JOIN datapoint DP
--     ON DP.MeasurementID = MR.MeasurementID


-- SELECT *
-- FROM measurementrecord
-- MeasurementRecordID
-- DateTimeRecorded
-- URNumber
-- MeasurementID
-- CategoryID


-- GO


-- this worked to select required data
-- BEGIN

--     DECLARE @CONNECTIONSTRING NVARCHAR(MAX);
--     EXECUTE @CONNECTIONSTRING = GET_CONNECTION_STRING;


--     DECLARE @SELECTQUERY0 NVARCHAR(MAX);

--     SET @SELECTQUERY0 = 
--                    '''SELECT MR.MeasurementRecordID, MR.DateTimeRecorded, MR.URNumber, ' + 
--                     'MR.MeasurementID, MR.CategoryID, DPR.DATAPOINTNUMBER, DPR.VALUE, ' + 
--                     'DP.[NAME], DP.LOWERLIMIT, DP.UPPERLIMIT ' + 
--                     'FROM DDDM_TPS_1.dbo.measurementrecord MR ' + 
--                     'INNER JOIN DDDM_TPS_1.dbo.datapointrecord DPR ' + 
--                     'ON MR.MeasurementRecordID = DPR.MeasurementRecordID ' + 
--                     'INNER JOIN DDDM_TPS_1.dbo.datapoint DP ' + 
--                     'ON DP.MeasurementID = MR.MeasurementID''';

--     DECLARE @COMMAND NVARCHAR(MAX);
--     SET @COMMAND = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY0 + ');'

--     PRINT('---- this is the command ----  ' + @COMMAND);
--     EXECUTE(@COMMAND);

-- END














