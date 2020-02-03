CREATE TABLE public.RA_FINAL
(
  CUSTOMERKEY                INTEGER,
  COLLECTIONKEY              INTEGER         NOT NULL,
  MARKETINGREGION            VARCHAR(40),
  BILLINGCOUNTRY             VARCHAR(40),
  CURRENCYCODE               VARCHAR(4),
  AMOUNT                     DECIMAL(15,2),
  REQUESTDATE                DATE,
  PROCESSORGROUP             VARCHAR(1),
  CARD_TYPE                  VARCHAR(20),
  DAYSTOCOLLECT              INTEGER,
  DECLINESUNTILSUCCESSFUL    INTEGER,
  ATTEMPTNUMBERINGROUP       INTEGER,
  COLLECTIONGROUPKEY         INTEGER         NOT NULL,
  NEXTGROUPKEY               INTEGER,
  COLLECTIONGROUPSTARTDATE   DATE,
  COLLECTIONGROUP            INTEGER,
  ATTEMPTSINGROUP            INTEGER,
  FINALCOLLECTIONATTEMPTKEY  INTEGER,
  GROUP_DISPOSITION          VARCHAR(9),
  GROUP_DISPOSITION_DETAIL   VARCHAR(86)
)

copy
public.RA_FINAL
from 's3://j2bi-s3-export/NewFile_23357'
delimiter ',' acceptinvchars '.' fillrecord emptyasnull blanksasnull ignoreheader 0 dateformat 'auto' region 'us-east-1' csv
iam_role 'arn:aws:iam::040022132465:role/dms-access-for-endpoint'

--select * from stl_load_errors order by starttime desc

--select * from ra_final