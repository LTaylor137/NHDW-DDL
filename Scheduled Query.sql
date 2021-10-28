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

-- SELECT *
-- FROM measurementrecord
-- SELECT *
-- FROM patientmeasurement
-- SELECT *
-- FROM datapointrecord
-- SELECT *
-- FROM measurement



-- CREATE DATABASE NHDW_LDT_0214;

-- USE NHDW_LDT_0214;



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------



-- SET GLOBAL event_scheduler = ON;
-- and create an event like this:

-- CREATE EVENT name_of_event
-- ON SCHEDULE EVERY 1 DAY
-- STARTS '2014-01-18 00:00:00'
-- DO
-- DELETE FROM tbl_message WHERE DATEDIFF( NOW( ) ,  timestamp ) >=7;



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------




GO

-- CREATE GET CONNECTION STRING FUNCTION.
USE NHDW_LDT_0214;






--------------------------------------------------------------------------------
------------------  Testing Selects on DW_DWDATAPOINTRECORD  -------------------
--------------------------------------------------------------------------------


SELECT *
FROM NHDW_LDT_0214.DBO.DW_PATIENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_MEASUREMENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD

SELECT *
FROM NHDW_LDT_0214.DBO.ERROR_EVENT

SELECT *
FROM NHDW_LDT_0214.DBO.DW_DIM_DATE


-- select all from DW_DWDATAPOINTRECORD
SELECT *
FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD DPR
INNER JOIN NHDW_LDT_0214.DBO.DW_PATIENT P
ON P.DWPATIENTID = DPR.DWPATIENTID
INNER JOIN NHDW_LDT_0214.DBO.DW_MEASUREMENT M
ON M.DWMEASUREMENTID = DPR.DWMEASUREMENTID


-- select all from DW_DWDATAPOINTRECORD where specific year and quarter.
SELECT *
FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD DPR
INNER JOIN NHDW_LDT_0214.DBO.DW_PATIENT P
ON P.DWPATIENTID = DPR.DWPATIENTID
INNER JOIN NHDW_LDT_0214.DBO.DW_MEASUREMENT M
ON M.DWMEASUREMENTID = DPR.DWMEASUREMENTID
INNER JOIN NHDW_LDT_0214.DBO.DW_DIM_DATE DD
ON DD.DateKey = DPR.DWDATEKEY
WHERE DD.DateKey = (SELECT DateKey WHERE year = 2020 AND Quarter = 1 OR Quarter = 2)

-- select from DW_DWDATAPOINTRECORD where specific year, category, and value.
SELECT P.DWPATIENTID, M.DWMEASUREMENTID, DPR.[VALUE], DD.[Year], P.CATEGORYNAME
FROM NHDW_LDT_0214.DBO.DW_DWDATAPOINTRECORD DPR
INNER JOIN NHDW_LDT_0214.DBO.DW_PATIENT P
ON P.DWPATIENTID = DPR.DWPATIENTID
INNER JOIN NHDW_LDT_0214.DBO.DW_MEASUREMENT M
ON M.DWMEASUREMENTID = DPR.DWMEASUREMENTID
INNER JOIN NHDW_LDT_0214.DBO.DW_DIM_DATE DD
ON DD.DateKey = DPR.DWDATEKEY
WHERE DD.DateKey = (SELECT DateKey WHERE year = 2020)
AND P.CATEGORYNAME = 'Indwelling Pleural Catheter'
AND DPR.[VALUE] > 3








