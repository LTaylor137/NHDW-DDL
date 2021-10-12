-- Student ID: 103200214
-- Name: Lachlan Taylor
-- 1/9/2021
-- github repo https://github.com/LTaylor137/NHDW-DDL/tree/table_creation
-- branch table_creation


-- SELECT NAME FROM SYS.DATABASES;

-- CREATE DATABASE NHDW_LDT_0214;

-- USE NHDW_LDT_0214;

-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- SELECT * FROM DBO.DW_PATIENT;



------------------ DUMMY TABLE FOR TESTING  ------------------

DROP TABLE IF EXISTS LIST_OF_ROWNUMS;
GO

CREATE TABLE LIST_OF_ROWNUMS
(
    ROWNUM NVARCHAR(100),
);
GO

INSERT INTO LIST_OF_ROWNUMS
VALUES
    (900000005),
    (900000010),
    (900000015),
    (900000020);

-- SELECT * FROM LIST_OF_ROWNUMS;

BEGIN
    DECLARE @ROWNUMS NVARCHAR(MAX);
    SELECT @ROWNUMS = COALESCE(@ROWNUMS + ',', '') + ROWNUM
    FROM LIST_OF_ROWNUMS
    PRINT (@ROWNUMS);
END;

-- RESULTS IN STRING 900000005,900000010,900000015,900000020



------------------ DECLARE A TABLE TYPE ------------------


DROP TYPE IF EXISTS DEMOTABLETYPE;
GO
CREATE TYPE DEMOTABLETYPE AS TABLE
(
    URNUMBER NVARCHAR(100),
    DWPATIENTID NVARCHAR(256),
    SUBURB NVARCHAR(500)
);
GO  

-- DECLARE A VARIABLE

BEGIN

DECLARE @PATIENTTABLE DEMOTABLETYPE;

INSERT INTO @PATIENTTABLE
SELECT  URNUMBER, DWPATIENTID, SUBURB
FROM DW_PATIENT;

SELECT 4, * FROM @PATIENTTABLE;

END




------------------ ERROR EVENT TABLE ------------------


DROP TABLE IF EXISTS ERROR_EVENT;
GO

CREATE TABLE ERROR_EVENT
(
    ERRORID INTEGER IDENTITY(1,1),
    -- IDENTITY(1,1) means this will automatically assign ID numbers when entries are added, and they will icrement by 1 each time.
    SOURCE_ID NVARCHAR(50),
    SOURCE_DATABASE NVARCHAR(50),
    SOURCE_TABLE NVARCHAR(50),
    FILTERID INTEGER,
    [DATETIME] DATETIME,
    [ACTION] NVARCHAR(50),
    CONSTRAINT ERROREVENTACTION CHECK (ACTION IN ('SKIP','MODIFY','REPLACE'))
);
GO



------------------ DIMENSION TABLES ------------------



DROP TABLE IF EXISTS DW_PATIENT;
GO

CREATE TABLE DW_PATIENT
(
    DWPATIENTID NVARCHAR(50) NOT NULL,
    URNUMBER NVARCHAR(50) NOT NULL,
    DWSOURCEDB NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    GENDER NVARCHAR(6) NOT NULL
        CHECK(GENDER IN ('Male','Female','Other')),
    DOB DATE NOT NULL,
    SUBURB NVARCHAR(50) NOT NULL,
    POSTCODE NVARCHAR(4) NOT NULL,
    COUNTRYOFBIRTH NVARCHAR(50) NOT NULL,
    PREFFEREDLANGUAGE NVARCHAR(50) NOT NULL,
    LIVESALONE NVARCHAR(3) NOT NULL
        CHECK(LIVESALONE IN ('Yes','No')),
    ACTIVE NVARCHAR(3) NOT NULL
        CHECK(ACTIVE IN ('Yes','No')),
    CATEGORYID INTEGER NOT NULL,
    CATEGORYNAME NVARCHAR(255) NOT NULL,
    PROCEDUREDATE DATETIME NOT NULL,
    DIAGNOSIS NVARCHAR(500) NOT NULL,
    PRIMARY KEY (DWPATIENTID)
);
GO



DROP TABLE IF EXISTS DW_STAFF;
GO

CREATE TABLE DW_STAFF
(
    DWSTAFFID INTEGER NOT NULL,
    STAFFID INTEGER NOT NULL,
    DWSOURCEBD NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    ROLEID INTEGER NOT NULL,
    STAFFTYPE NVARCHAR(50) NOT NULL,
    PRIMARY KEY (DWSTAFFID)
);
GO



DROP TABLE IF EXISTS DW_MEASUREMENT;
GO

CREATE TABLE DW_MEASUREMENT
(
    DWMEASUREMENTID INTEGER NOT NULL,
    MEASUREMENTID INTEGER NOT NULL,
    DATAPOINTNUMBER INTEGER NOT NULL,
    DWSOURCEBD NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    MEASUREMENTNAME NVARCHAR(50) NOT NULL,
    [NAME] NVARCHAR(50) NOT NULL,
    UPPERLIMIT INTEGER NOT NULL,
    LOWERLIMIT INTEGER NOT NULL,
    QUESTION NVARCHAR(255) NOT NULL,
    ANSWERNUMBER INTEGER NULL,
    PRIMARY KEY (DWMEASUREMENTID)
);
GO



DROP TABLE IF EXISTS DW_RECORDTYPE;
GO

CREATE TABLE DW_RECORDTYPE
(
    DWRECORDTYPEID INTEGER NOT NULL,
    RECORDTYPEID INTEGER NOT NULL,
    RECORDCATEGORYID INTEGER NOT NULL,
    DWSOURCEBD NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    RECORDTYPE NVARCHAR(50) NOT NULL,
    [CATEGORY] INTEGER NOT NULL,
    PRIMARY KEY (DWRECORDTYPEID)
);
GO



DROP TABLE IF EXISTS DW_DATE;
GO

CREATE TABLE DW_DATE
(
    [DWDATETIMEKEY] INTEGER NOT NULL,
    PRIMARY KEY (DWDATETIMEKEY)
);
GO


------------------ FACT TABLES ------------------



DROP TABLE IF EXISTS DW_TREATING;
GO

CREATE TABLE DW_TREATING
(
    DWPATIENTID NVARCHAR(50) NOT NULL,
    DWSTAFFID INTEGER NOT NULL,
    DWDATETIMEKEY INTEGER NOT NULL,
    ENDDATE DATETIME NULL,
    PRIMARY KEY (DWPATIENTID, DWSTAFFID, DWDATETIMEKEY)
);
GO



DROP TABLE IF EXISTS DW_INTERVENTION;
GO

CREATE TABLE DW_INTERVENTION
(
    DWRECORDTYPEID INTEGER NOT NULL,
    DWPATIENTID NVARCHAR(50) NOT NULL,
    DWSTAFFID INTEGER NOT NULL,
    DWDATETIMEKEY DATETIME NOT NULL,
    [NOTES] NVARCHAR(500) NULL,
    PRIMARY KEY (DWRECORDTYPEID, DWPATIENTID, DWSTAFFID, DWDATETIMEKEY)
);
GO



DROP TABLE IF EXISTS DW_DWDATAPOINTRECORD;
GO

CREATE TABLE DW_DWDATAPOINTRECORD
(
    DWPATIENTID NVARCHAR(50) NOT NULL,
    DWMEASUREMENTID INTEGER NOT NULL,
    DWDATETIMEKEY INTEGER NOT NULL,
    [VALUE] FLOAT(10) NOT NULL,
    ANSWERTEXT NVARCHAR(255) NOT NULL,
    FREQUENCY INTEGER NOT NULL,
    PRIMARY KEY (DWPATIENTID, DWMEASUREMENTID, DWDATETIMEKEY)
);
GO





-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- SELECT * FROM DBO.DWPATIENT;
-- SELECT * FROM DBO.DWSTAFF;
-- SELECT * FROM DBO.DWDATAPOINT;
-- SELECT * FROM DBO.DWTREATING;
-- SELECT * FROM DBO.DWINTERVENTION;
-- SELECT * FROM DBO.DWMEASUREMENT;

