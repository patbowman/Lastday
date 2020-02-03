CREATE TABLE ETL.RAD_FINAL      --Perm
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
CREATE TABLE ETL.RETRY_ANALYSIS_FINAL --Perm
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

/