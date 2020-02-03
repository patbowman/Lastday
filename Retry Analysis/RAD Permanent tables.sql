
CREATE TABLE ODS_CENTRAL.RAD_Collection_Groups --Perm
(
Customerkey         NUMBER(11), 
CollectionGroup     NUMBER(11),
CollectionGroupStartDate DATE,
CollectionGroupKey NUMBER(11) ,
NEXTGROUPKEY       NUMBER(11),
NEXTGROUPSTARTDATE DATE,
final_status_flag  number(2), -- 1 final, 0 not final
PROCESS_DATETIME   DATE
);

CREATE TABLE ODS_CENTRAL.RAD_Collection_Group_Disp --Perm
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
CREATE TABLE ODS_CENTRAL.RAD_FINAL      --Perm
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

/
CREATE  TABLE ODS_CENTRAL.RETRY_ANALYSIS_FINAL --Perm
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
);
