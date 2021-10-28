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

------------------ ERROR EVENT TABLE ------------------


DROP TABLE IF EXISTS ERROR_EVENT;
GO

CREATE TABLE ERROR_EVENT
(
    ERRORID INTEGER IDENTITY(1,1),
    SOURCE_ID NVARCHAR(50),
    SOURCE_DATABASE NVARCHAR(50),
    SOURCE_TABLE NVARCHAR(50),
    FILTERID NVARCHAR(5),
    [DATETIME] DATETIME,
    [ACTION] NVARCHAR(50),
    CONSTRAINT ERROREVENTACTION CHECK (ACTION IN ('SKIP','MODIFY','REPLACE'))
);
GO



------------------ GENDER SPELLING TABLE ------------------

SELECT * FROM INFORMATION_SCHEMA.TABLES;

USE NHDW_LDT_0214;

DROP TABLE IF EXISTS GENDERSPELLING;
GO

CREATE TABLE GENDERSPELLING (
    INVALID_VALUE VARCHAR(30),
    NEW_VALUE NVARCHAR(10)
);

INSERT INTO GENDERSPELLING (INVALID_VALUE, NEW_VALUE) VALUES
('MAIL', 'MALE'),
('WOMAN', 'FEMALE'),
('FEM', 'FEMALE'),
('FEMALE', 'FEMALE'),
('MALE', 'MALE'),
('GENTELMAN', 'MALE'),
('M', 'MALE'), 
('MM', 'MALE'), 
('F', 'FEMALE'),
('FF', 'FEMALE'),
('FEMAIL', 'FEMALE');


------------------ DIMENSION TABLES ------------------

USE NHDW_LDT_0214;
GO
DROP TABLE IF EXISTS DW_PATIENT;
GO

CREATE TABLE DW_PATIENT
(
    DWPATIENTID INTEGER IDENTITY(1,1),
    URNUMBER NVARCHAR(50) NOT NULL,
    DWSOURCEDB NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    GENDER NVARCHAR(6) NOT NULL
        CHECK(GENDER IN ('MALE','FEMALE')),
    DOB INT NOT NULL,
    SUBURB NVARCHAR(50) NOT NULL,
    POSTCODE NVARCHAR(4) NOT NULL,
    COUNTRYOFBIRTH NVARCHAR(50) NOT NULL,
    LIVESALONE NVARCHAR(1) NOT NULL
        CHECK(LIVESALONE IN ('1','0')),
    ACTIVE NVARCHAR(1) NOT NULL
        CHECK(ACTIVE IN ('1','0')),
    CATEGORYNAME NVARCHAR(255)  NULL,
    PROCEDUREDATE NVARCHAR(255)  NULL,
    DIAGNOSIS NVARCHAR(500)  NULL,
    PRIMARY KEY (DWPATIENTID)
);
GO



DROP TABLE IF EXISTS DW_STAFF;
GO

CREATE TABLE DW_STAFF
(
    DWSTAFFID INTEGER IDENTITY(1,1),
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
    DWMEASUREMENTID INTEGER IDENTITY(1,1),
    MEASUREMENTRECORDID NVARCHAR(50) NOT NULL,
    DATETIMERECORDED DATETIME NOT NULL,
    URNUMBER NVARCHAR(50) NOT NULL,
    DWSOURCEBD NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    MEASUREMENTID NVARCHAR(50) NOT NULL,
    DATAPOINTNUMBER INTEGER NOT NULL,
    CATEGORYID NVARCHAR(50) NOT NULL,
    MEASUREMENTNAME NVARCHAR(50) NOT NULL,
    MEASUREMENTVALUE NVARCHAR(50) NOT NULL,
    UPPERLIMIT INTEGER NOT NULL,
    LOWERLIMIT INTEGER NOT NULL,
    -- QUESTION NVARCHAR(255) NOT NULL,
    -- ANSWERNUMBER INTEGER NULL,
    PRIMARY KEY (DWMEASUREMENTID)
);
GO



DROP TABLE IF EXISTS DW_RECORDTYPE;
GO

CREATE TABLE DW_RECORDTYPE
(
    DWRECORDTYPEID INTEGER IDENTITY(1,1),
    RECORDTYPEID INTEGER NOT NULL,
    RECORDCATEGORYID INTEGER NOT NULL,
    DWSOURCEBD NVARCHAR(50) NOT NULL,
    DWSOURCETABLE NVARCHAR(50) NOT NULL,
    RECORDTYPE NVARCHAR(50) NOT NULL,
    [CATEGORY] INTEGER NOT NULL,
    PRIMARY KEY (DWRECORDTYPEID)
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




USE NHDW_LDT_0214;



DROP TABLE IF EXISTS DW_DWDATAPOINTRECORD;
GO

CREATE TABLE DW_DWDATAPOINTRECORD
(
    DWPATIENTID NVARCHAR(50) NOT NULL,
    DWMEASUREMENTID INTEGER NOT NULL,
    DWDATEKEY INTEGER NOT NULL,
    [VALUE] FLOAT(10) NOT NULL,
    FREQUENCYDATEKEY INTEGER NOT NULL,
    FREQUENCY INTEGER NOT NULL,
    PRIMARY KEY (DWPATIENTID, DWMEASUREMENTID, DWDATEKEY)
);
GO




-- SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- SELECT * FROM DBO.DWPATIENT;
-- SELECT * FROM DBO.DWSTAFF;
-- SELECT * FROM DBO.DWDATAPOINT;
-- SELECT * FROM DBO.DWTREATING;
-- SELECT * FROM DBO.DWINTERVENTION;
-- SELECT * FROM DBO.DWMEASUREMENT;

