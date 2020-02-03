
CREATE TABLE PBOWMAN.RAD_NewAttempts_EX1 -- Temp
(
  CUSTOMERKEY       NUMBER(11),
  COLLECTIONKEY     NUMBER(11)                  NOT NULL,
  RESPONSE          VARCHAR2(1 CHAR),
  RESPONSECODE      VARCHAR2(20 CHAR),
  PROCESSORGROUP    VARCHAR2(1 CHAR),
  REQUESTDATE       DATE,
  CARD_TYPE         VARCHAR2(20 CHAR),
  CURRENCYCODE      VARCHAR2(4 CHAR),
  AMOUNT            NUMBER(15,2),
  BILLINGCOUNTRY    VARCHAR2(40 CHAR),
  MARKETINGREGION   VARCHAR2(40 CHAR),
  RETRY_COUNT       NUMBER(11),
  EXCLUSION_REASON  VARCHAR2(9 CHAR),
  NEW_GROUP_FLAG    NUMBER(11), -- 1 for new group, 0 if not new grou,
  COLLECTED_FLAG    NUMBER(11) -- 1 for Collected, 0 if not
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;

CREATE TABLE PBOWMAN.RAD_DISPATTEMPTS_EX2
(
  COLLECTIONKEY     NUMBER(11)                  NOT NULL,
  CUSTOMERKEY       NUMBER(11),
  RESPONSE          VARCHAR2(1 BYTE),
  PROCESSORGROUP    VARCHAR2(1 BYTE),
  RETRYCOUNT        NUMBER(5),
  REQUESTDATE       DATE,
  AMOUNT            NUMBER(15,2),
  ODS_CURRENT_FLAG  NUMBER(1)                   NOT NULL
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;


CREATE TABLE PBOWMAN.RAD_AttemptGroupAssoc_EX2 -- Temp
(
Customerkey         NUMBER(11), 
Collectionkey       NUMBER(11), 
CollectionGroupKey     NUMBER(11)
)
/
CREATE TABLE PBOWMAN.RAD_NEWATTEMPTS_STATS_EX3
(
  COLLECTIONGROUPKEY  NUMBER(11),
  NEWATTEMPTS         NUMBER,
  MOSTRECENTATTEMPT   NUMBER
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING
/

CREATE TABLE PBOWMAN.RAD_New_Collection_Groups -- Temp
(
Customerkey         NUMBER(11), 
CollectionGroup     NUMBER(11),
CollectionGroupStartDate DATE,
CollectionGroupKey NUMBER(11) NOT NULL
)
/
CREATE TABLE PBOWMAN.RAD_ACTIVE_GROUPS_EX1
(
  CUSTOMERKEY               NUMBER(11),
  COLLECTIONGROUP           NUMBER(11),
  COLLECTIONGROUPSTARTDATE  DATE,
  COLLECTIONGROUPKEY        NUMBER(11)          NOT NULL,
  NEXTGROUPKEY              NUMBER(11),
  NEXTGROUPSTARTDATE        DATE,
  FINAL_STATUS_FLAG         NUMBER,
  STATUS                    VARCHAR2(1 BYTE)
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;


CREATE TABLE PBOWMAN.RAD_NEW_COLLECTION_GROUPS_EX3
(
  CUSTOMERKEY      NUMBER(11),
  PREVCOLLECTIONGROUP  NUMBER
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;


CREATE TABLE PBOWMAN.RAD_New_Collection_Groups_EX1 -- Temp
(
Customerkey         NUMBER(11), 
CollectionGroup     NUMBER(11),
CollectionGroupStartDate DATE,
 CollectionGroupKey NUMBER(11) NOT NULL,
 NEXTGROUPKEY       NUMBER(11),
 NEXTGROUPSTARTDATE DATE
)
/
CREATE TABLE PBOWMAN.RAD_NEW_GROUP_STATS -- Temp
(
  CUSTOMERKEY               NUMBER(11),
  COLLECTIONGROUP           NUMBER(11),
  COLLECTIONGROUPSTARTDATE  DATE,
  COLLECTIONGROUPKEY        NUMBER(11)          NOT NULL,
  NEXTGROUPKEY              NUMBER(11),
  NEXTGROUPSTARTDATE        DATE,
  NEWATTEMPTS               NUMBER,
  MOSTRECENTATTEMPT         NUMBER,
  COLLECTED_FLAG            NUMBER(11),
  NEW_GROUP_FLAG            NUMBER(11)
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;


CREATE BITMAP INDEX PBOWMAN.RAD_NEWGROUP_NG_BI ON PBOWMAN.RAD_NEW_GROUP_STATS
(NEW_GROUP_FLAG)
LOGGING
TABLESPACE USERS_NEW
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;


CREATE BITMAP INDEX PBOWMAN.RAD_NEW_GROUP_CF_BI ON PBOWMAN.RAD_NEW_GROUP_STATS
(COLLECTED_FLAG)
LOGGING
TABLESPACE USERS_NEW
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL

/
--DROP TABLE PBOWMAN.RAD_NEWATTEMPTS_STATS_EX5 CASCADE CONSTRAINTS;
/*
CREATE TABLE PBOWMAN.RAD_NEWATTEMPTS_STATS_EX5
(
  COLLECTIONGROUPKEY  NUMBER(11),
  NEWATTEMPTS         NUMBER,
  MOSTRECENTATTEMPT   NUMBER
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;

*/


CREATE TABLE PBOWMAN.RAD_Collection_Groups --Perm
(
Customerkey         NUMBER(11), 
CollectionGroup     NUMBER(11),
CollectionGroupStartDate DATE,
CollectionGroupKey NUMBER(11) NOT NULL,
NEXTGROUPKEY       NUMBER(11),
NEXTGROUPSTARTDATE DATE,
final_status_flag  number, -- 1 final, 0 not final
PROCESS_DATETIME   DATE
)
/

CREATE BITMAP INDEX PBOWMAN.RAD_FINALSTATUS_BI ON PBOWMAN.RAD_COLLECTION_GROUPS
(FINAL_STATUS_FLAG)
LOGGING
TABLESPACE USERS_NEW
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL
/

CREATE TABLE PBOWMAN.RAD_Collection_Group_Disp_CF1 --Temp
(
  CUSTOMERKEY               NUMBER(11),
  COLLECTIONGROUP           NUMBER,
  COLLECTIONGROUPSTARTDATE  DATE,
  COLLECTIONGROUPKEY        NUMBER(11)          NOT NULL,
  NEXTGROUPKEY              NUMBER,
  NEXTGROUPSTARTDATE        DATE,
  ATTEMPTSINGROUP           NUMBER,
  FINALATTEMPT              NUMBER, 
--are these group attributes or attempt attributes?
--(I think they're being requested for attempts but actually they are group)
 daystocollect              NUMBER,

 declinesuntilsuccessful    NUMBER,
Group_Disposition       varchar2(100 char),
 Group_Disposition_Detail varchar2(200 char),
 final_status_flag  number, -- 1 final, 0 not final
PROCESS_DATETIME   DATE
)
/

CREATE TABLE PBOWMAN.RAD_Collection_Group_Disp --Perm
(
  CUSTOMERKEY               NUMBER(11),
  COLLECTIONGROUP           NUMBER,
  COLLECTIONGROUPSTARTDATE  DATE,
  COLLECTIONGROUPKEY        NUMBER(11)          NOT NULL,
  NEXTGROUPKEY              NUMBER,
  NEXTGROUPSTARTDATE        DATE,
  ATTEMPTSINGROUP           NUMBER,
  FINALATTEMPT              NUMBER, 
--are these group attributes or attempt attributes?
--(I think they're being requested for attempts but actually they are group)
 daystocollect              NUMBER,

 declinesuntilsuccessful    NUMBER,
Group_Disposition       varchar2(100 char),
 Group_Disposition_Detail varchar2(200 char),
   FINAL_STATUS_FLAG          NUMBER,
  PROCESS_DATETIME           DATE
 
)
/
CREATE TABLE PBOWMAN.RAD_FINAL      --Perm
(
  CUSTOMERKEY                NUMBER(11),
  COLLECTIONKEY              NUMBER(11)         NOT NULL,
  MARKETINGREGION            VARCHAR2(40 CHAR),
  BILLINGCOUNTRY             VARCHAR2(40 CHAR),
  CURRENCYCODE               VARCHAR2(4 CHAR),
  AMOUNT                     NUMBER(15,2),
  REQUESTDATE                DATE,
  PROCESSORGROUP             VARCHAR2(1 CHAR),
  CARD_TYPE                  VARCHAR2(20 CHAR),
  DAYSTOCOLLECT              NUMBER,
  DECLINESUNTILSUCCESSFUL    NUMBER,
  ATTEMPTNUMBERINGROUP       NUMBER,
  COLLECTIONGROUPKEY         NUMBER(11)         NOT NULL,
  NEXTGROUPKEY               NUMBER,
  COLLECTIONGROUPSTARTDATE   DATE,
  COLLECTIONGROUP            NUMBER,
  ATTEMPTSINGROUP            NUMBER,
  FINALCOLLECTIONATTEMPTKEY  NUMBER,
  GROUP_DISPOSITION          VARCHAR2(100 CHAR),
  GROUP_DISPOSITION_DETAIL   VARCHAR2(200 CHAR),
  FINAL_STATUS_FLAG          NUMBER,
  PROCESS_DATETIME           DATE
  
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING
/
CREATE TABLE PBOWMAN.RETRY_ANALYSIS_FINAL --Perm
(
  CUSTOMERKEY                NUMBER(11),
  COLLECTIONKEY              NUMBER(11)         NOT NULL,
  MARKETINGREGION            VARCHAR2(40 CHAR),
  BILLINGCOUNTRY             VARCHAR2(40 CHAR),
  CURRENCYCODE               VARCHAR2(4 CHAR),
  AMOUNT                     NUMBER(15,2),
  REQUESTDATE                DATE,
  PROCESSORGROUP             VARCHAR2(1 CHAR),
  CARD_TYPE                  VARCHAR2(20 CHAR),
  DAYSTOCOLLECT              NUMBER,
  DECLINESUNTILSUCCESSFUL    NUMBER,
  ATTEMPTNUMBERINGROUP       NUMBER,
  COLLECTIONGROUPKEY         NUMBER(11)         NOT NULL,
  NEXTGROUPKEY               NUMBER,
  COLLECTIONGROUPSTARTDATE   DATE,
  COLLECTIONGROUP            NUMBER,
  ATTEMPTSINGROUP            NUMBER,
  FINALCOLLECTIONATTEMPTKEY  NUMBER,
  GROUP_DISPOSITION          VARCHAR2(100 CHAR),
  GROUP_DISPOSITION_DETAIL   VARCHAR2(200 CHAR)
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING
/
CREATE TABLE PBOWMAN.RETRY_ANALYSIS_ACTIVE --Perm
(
  CUSTOMERKEY                NUMBER(11),
  COLLECTIONKEY              NUMBER(11)         NOT NULL,
  MARKETINGREGION            VARCHAR2(40 CHAR),
  BILLINGCOUNTRY             VARCHAR2(40 CHAR),
  CURRENCYCODE               VARCHAR2(4 CHAR),
  AMOUNT                     NUMBER(15,2),
  REQUESTDATE                DATE,
  PROCESSORGROUP             VARCHAR2(1 CHAR),
  CARD_TYPE                  VARCHAR2(20 CHAR),
  DAYSTOCOLLECT              NUMBER,
  DECLINESUNTILSUCCESSFUL    NUMBER,
  ATTEMPTNUMBERINGROUP       NUMBER,
  COLLECTIONGROUPKEY         NUMBER(11)         NOT NULL,
  NEXTGROUPKEY               NUMBER,
  COLLECTIONGROUPSTARTDATE   DATE,
  COLLECTIONGROUP            NUMBER,
  ATTEMPTSINGROUP            NUMBER,
  FINALCOLLECTIONATTEMPTKEY  NUMBER,
  GROUP_DISPOSITION          VARCHAR2(100 CHAR),
  GROUP_DISPOSITION_DETAIL   VARCHAR2(200 CHAR)
)
TABLESPACE USERS_NEW
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          160K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING
/
/*
select * from PBOWMAN.RAD_NewAttempts
select * from PBOWMAN.RAD_New_Collection_Groups
/
select * from PBOWMAN.RAD_New_Collection_Groups_EX1 
/
select * from PBOWMAN.RAD_NEW_GROUP_STATS
/
select * from PBOWMAN.RAD_Active_Collection_Groups 
/
select * from PBOWMAN.RAD_Final_Collection_Groups 
/
select * from PBOWMAN.RAD_Active_Collection_Groups 
/
select * from PBOWMAN.RAD_Collection_Group_Disp
/

create view  RA_Collection_Groups as -- Perm..
select * from RAD_Final_Collection_Groups 
union all -- shouldn't make a difference, and UNION ALL is supposed to be faster
select * from RAD_Active_Collection_Groups


CREATE TABLE PBOWMAN.RAD_Final_Collection_Groups --Perm
(
Customerkey         NUMBER(11), 
CollectionGroup     NUMBER(11),
CollectionGroupStartDate DATE,
 CollectionGroupKey NUMBER(11) NOT NULL,
 NEXTGROUPKEY       NUMBER(11),
 NEXTGROUPSTARTDATE DATE
)
/

CREATE TABLE PBOWMAN.RAD_Active_Collection_Groups --Perm
(
Customerkey         NUMBER(11), 
CollectionGroup     NUMBER(11),
CollectionGroupStartDate DATE,
 CollectionGroupKey NUMBER(11) NOT NULL,
 NEXTGROUPKEY       NUMBER(11),
 NEXTGROUPSTARTDATE DATE
)
/

*/