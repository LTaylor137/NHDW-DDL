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
FROM datapoint

--------------------------------------------------------------------------------
----------------------------- General table lookups ----------------------------
-------------------------------------------------------------------------------

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
--------------------------------- Problem 2 Get the required data from the source. -------------------------------------
------------------------------------------------------------------------------------------------------------------------


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
    DWDATEKEY INTEGER NOT NULL,
    SCR_VALUE FLOAT(10) NOT NULL,
    FREQUENCYDATEKEY INTEGER NOT NULL,
    FREQUENCY INTEGER NOT NULL
);



--------------------------------------------------------------------------------
------------------------------ Transfer Procedure ------------------------------
--------------------------------------------------------------------------------


-- pull data from DDDM_TPS_1.DBO.PATIENT and insert it into a temptable. 
DROP PROCEDURE IF EXISTS ETL_PROCEDURE_DWDATAPOINTRECORD
GO
CREATE PROCEDURE ETL_PROCEDURE_DWDATAPOINTRECORD
AS
BEGIN

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
    SET @COMMAND_DPR = 'SELECT * FROM OPENROWSET(''SQLNCLI'', ' + '''' + @CONNECTIONSTRING + ''',' + @SELECTQUERY03 + ');'

    PRINT('---- this is the command ----  ' + @COMMAND_DPR);

    DECLARE @TEMPDATAPOINTRECORDTABLE AS TEMP_DATAPOINTRECORD_TABLE_TYPE;

    SELECT 'TT DPR A', *
    FROM @TEMPDATAPOINTRECORDTABLE;

    INSERT INTO @TEMPDATAPOINTRECORDTABLE
    EXECUTE(@COMMAND_DPR);

    SELECT 'TT DPR B', *
    FROM @TEMPDATAPOINTRECORDTABLE;

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





----------------------------------------------------------------------------------------
------------------------------- Transfer into DW PATIENT -------------------------------
----------------------------------------------------------------------------------------
-- Problem 5 insert the good data

-- The idea is to transfer any remaining data to the patient table, 
-- after it has been modified, or removed.


USE NHDW_LDT_0214;



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
        DWPATIENTID,
        DWMEASUREMENTID,
        DWDATEKEY,
        [VALUE],
        FREQUENCYDATEKEY,
        FREQUENCY
        )
    SELECT
        DWP.DWPATIENTID,
        DWM.DWMEASUREMENTID,
        DWDD.DateKey,
        D.SCR_VALUE,
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





