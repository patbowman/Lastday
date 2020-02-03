-- Drop table

-- DROP TABLE public.ra_final_disp

CREATE TABLE IF NOT EXISTS public.rp_ra_stage
(
	customerkey INTEGER   ENCODE lzo
	,collectionkey INTEGER NOT NULL  ENCODE lzo
	,marketingregion VARCHAR(40)   ENCODE lzo
	,billingcountry VARCHAR(40)   ENCODE lzo
	,currencycode VARCHAR(4)   ENCODE lzo
	,amount NUMERIC(15,2)   ENCODE lzo
	,requestdate DATE   ENCODE lzo
	,processorgroup VARCHAR(1)   ENCODE lzo
	,card_type VARCHAR(20)   ENCODE lzo
	,daystocollect  NUMERIC(15,7)   ENCODE lzo
	,declinesuntilsuccessful NUMERIC(15,7)     ENCODE lzo
	,attemptnumberingroup NUMERIC(15,7)     ENCODE lzo
	,collectiongroupkey INTEGER NOT NULL  ENCODE lzo
	,nextgroupkey NUMERIC(20,7)     ENCODE lzo
	,collectiongroupstartdate DATE   ENCODE lzo
	,collectiongroup NUMERIC(15,7)     ENCODE lzo
	,attemptsingroup NUMERIC(15,7)     ENCODE lzo
	,finalcollectionattemptkey  NUMERIC(20,7)     ENCODE lzo
	,group_disposition VARCHAR(9)   ENCODE lzo
	,group_disposition_detail VARCHAR(86)   ENCODE lzo
	,ods_current_flag NUMERIC(15,7) 
	,process_date DATE ENCODE lzo
	--,collected_amount         DECIMAL(15,2)
)
truncate table public.ra_stage;
--delete from public.ra_stage where requestdate >'01-JAN-2018'
copy
public.rp_ra_stage
from 's3://j2bi-s3-export/RP_RETRYANALYSIS_2017'
delimiter ',' acceptinvchars '.' fillrecord emptyasnull blanksasnull ignoreheader 0 dateformat 'auto' region 'us-east-1' csv
iam_role 'arn:aws:iam::040022132465:role/dms-access-for-endpoint'

insert public.rp_retryanalysis

select customerkey, collectionkey, marketingregion, billingcountry, currencycode, amount, requestdate, prc
processorgroup, card_type, daystocollect, declinesuntilsuccessful, attemptnumberingroup, collectiongroupkey
nextgroupkey, collectiongroupstartdate, collectiongroup,
attemptsingroup, finalcollectionattemptkey, 
group_disposition, group_disposition_detail,
CASE when 
public.ra_stage