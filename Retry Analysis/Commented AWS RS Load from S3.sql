-- Loading Redshift with Retry Analysis data
--
-- I set up a staging table because there was a final addition
-- and it seemed the simplest way to deal with it.
-- You don't have to drop and recreate the table, but it's an option.

-- Drop table public.rp_ra_stage

/*

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
*/
--
-- But you will want to truncate any data left there
--
truncate table public.rp_ra_stage;
--
-- Here's the important part: Redshift COPY command.
-- Be sure to change the name of the file 
-- (to the file you previously exported, that is)
--
copy
public.rp_ra_stage
from 's3://j2bi-s3-export/RP_RETRYANALYSIS_20200115'
delimiter ',' acceptinvchars '.' fillrecord emptyasnull blanksasnull ignoreheader 0 dateformat 'auto' region 'us-east-1' csv
iam_role 'arn:aws:iam::040022132465:role/dms-access-for-endpoint'
--
-- Check your count - should be as many rows as in the source file
--

select count(*) from public.rp_ra_stage
--
-- Like I said, there's this other field we are adding
-- Want to do it in a different way, well, knock yourself out
-- Really, we could run all of his with minimal changes completely within airflow
-- But this is how it goes, sometimes
--

--
-- And finally, actually insert
-- 
-- With a case statement to load COLLECTED_amount if it represents an actual collection
-- If it's a decline or whatever, don't - only on the collection for this group
-- Determined by DaysToCollect, which is null if it has not been collected
-- (Use a different method if you like
-- did it like this because it's a simple option at this step.)
--
insert into public.rp_retryanalysis

select customerkey, collectionkey, marketingregion, billingcountry, currencycode, amount, requestdate,
processorgroup, card_type, daystocollect, declinesuntilsuccessful, attemptnumberingroup, collectiongroupkey,
nextgroupkey, collectiongroupstartdate, collectiongroup,
attemptsingroup, finalcollectionattemptkey, 
group_disposition, group_disposition_detail,
CASE when daystocollect is null then 0 else amount END Collected_Amount,
ods_current_flag, process_date
from public.rp_ra_stage

--
-- That should do it.
--