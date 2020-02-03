-- two minutes, 9.2M rows
-- filtering out preauths, it's 8.1M rowas
create table RAI_custsubset as
select customerkey, collectionkey, response, responsecode, processorgroup, requestdate, 
 --collectiondate,
 typecode Card_Type, 
 currencycode, amount,
 country billingcountry,marketingregion,
 CASE when  nvl(preauthflag,0) in (-1,1) then 'Preauth' when response='C' then 'Cancelled' else null END Exclusion_reason 
-- ods_central.j2_ispcollection co 
 --co.*
  from ods_central.j2_ispcollection co
LEFT OUTER JOIN ods_central.dw_country ON co.country=DW_COUNTRY.COUNTRYCODE and dw_country.ods_current_flag=1
  where   requestdate >= '13-AUG-2019' 
  and requestdate < '03-OCT-2019'
 --between '01-JUL-2019' and '02-JUL-2019'
 --and nvl(preauthflag,0) not in (-1, 1) -- no preauths here
  and co.ods_current_flag = 1;
  
  
  --44 minutes
-- 124M rows...although it's small, so.

create table RAI_cgroups as
select distinct customerkey, requestdate collectiongroupstartdate, collectionkey collectiongroupkey,         
dense_rank() over (partition by customerkey order by collectionkey)        
 collectiongroup         
from  ods_central.j2_ispcollection co        
where         
processorgroup in ('Z','B','R','O') -- tellyawhat, to start, let's ignore P too        
and retrycount = 0        
and ods_current_flag  = 1    
   -- and customerkey in (select customerkey from custsubset)    
and response not in ('C')     -- ignore cancelled attempts for this purpose - may reconsider.
 -- and co.customerkey in ( 40763324,49464114) 
order by collectionkey;

-- 12:04 minutes
--124M rows
create table RAI_nextcgroups as        
(select customerkey, collectiongroup, collectiongroupstartdate, collectiongroupkey,        
max(collectiongroupkey) OVER (partition by customerkey order by collectiongroup RANGE BETWEEN 0 preceding and 1 following)  NextGroupKey,        
max(CollectionGroupStartDate) OVER (partition by customerkey order by collectiongroup RANGE BETWEEN 0 preceding and 1 following)  NextGroupStartDate        
from RAI_cgroups        
);

-- 2:05:09 hours
--124M rows
create table RAI_cgroupcounts as        
(select distinct rai_nextcgroups.*,         
count(collectionkey) OVER (partition by RAI_nextcgroups.customerkey, collectiongroup) AttemptsInGroup,        
max(collectionkey) OVER (partition by RAI_nextcgroups.customerkey, collectiongroup) FinalAttempt 
from RAI_nextcgroups         
JOIN ods_central.j2_ISPCOLLECTION co ON RAI_nextcgroups.customerkey = co.customerkey and collectionkey >= collectiongroupkey         
       and (collectionkey < nextgroupkey or collectiongroupkey = nextgroupkey) -- include through the next group, or include any if it's the last one        
        
where ods_current_flag  = 1        
and response not in ('C')       
and nvl(preauthflag,0) not in (-1, 1)   -- don't count pre-auths for this 
);

--1:19:18 hours. Not bad.
--124M rows
create table RAI_cgroupsdisp as        
(select distinct  RAI_cgroupcounts.*,    
--are these group attributes or attempt attributes?
--(I think they're being requested for attempts but actually they are group)
CASE when response = 'D' then null else     trunc( requestdate)-collectiongroupstartdate END
 daystocollect,
 CASE when response = 'D' then null else      attemptsingroup-1 END
       declinesuntilsuccessful ,   
CASE when DISP.RESPONSE='A'  then 'Paid'        
when DISP.RESPONSE='D' and retrycount < 4 and collectiongroup = max(collectiongroup) OVER (partition by RAI_cgroupcounts.customerkey) then 'Declining'        
else 'Failed' END Group_Disposition,        
CASE when DISP.RESPONSE='A' and retrycount =0 and processorgroup in ('B','Z') then 'Paid on first attempt'        
when DISP.RESPONSE='A' and retrycount > 0 and processorgroup in ('B','Z') then 'Paid on batch try '||to_char(retrycount+1)        
when DISP.RESPONSE='A' and processorgroup='I' then 'Paid on info update try '||to_char(retrycount+1)        
when DISP.RESPONSE='A' and processorgroup='C' then 'Paid with CS help try '||to_char(retrycount+1)        
when DISP.RESPONSE='A' and processorgroup='R' then 'Paid Realtime try '||to_char(retrycount+1)        
when DISP.RESPONSE='A' and processorgroup='O' then 'Paid Topup Process try '||to_char(retrycount+1)        
when DISP.RESPONSE='A' then 'Paid non-batch try '||to_char(retrycount+1)        
when DISP.RESPONSE='D' and retrycount < 4 and collectiongroup = max(collectiongroup) OVER (partition by RAI_cgroupcounts.customerkey) then 'Declining through try '||to_char(retrycount+1) || ' (Attempt in group  TBD)'      
else 'Failed' END Group_Disposition_Detail        
 --, max(collectionkey) OVER (partition by cgroups2.customerkey, collectiongroup) FinalAttempt         
from  RAI_cgroupcounts         
JOIN ods_central.j2_ISPCOLLECTION disp ON RAI_cgroupcounts.customerkey = disp.customerkey and collectionkey=finalattempt-->= collectiongroupkey and collectionkey < nextgroupkey        
where ods_current_flag  = 1    

and response not in ('C')        
);

--
-- Final Result Set
-- 
  
 create table RAI_Final as 
    select
   RAI_custsubset.customerkey,
   RAI_custsubset.collectionkey,
    MarketingRegion,
    BillingCountry,
    currencycode,
    amount,
   requestdate,
  -- collectiondate,it's in here but doesn't DO anything.
   processorgroup,
   CARD_TYPE,
   daystocollect,
   declinesuntilsuccessful,
   

            dense_rank() over (partition by collectiongroupkey order by collectionkey) AttemptNumberinGroup,
       --collectionkey, 
       RAI_cgroupsdisp.collectiongroupkey , RAI_cgroupsdisp.nextgroupkey,
     --  RA_cgroupsdisp.* --, custsubset.*
RAI_CGROUPSDISP.COLLECTIONGROUPSTARTDATE,  
RAI_CGROUPSDISP.COLLECTIONGROUP,
RAI_CGROUPSDISP.AttemptsInGroup,
RAI_CGROUPSDISP.FinalAttempt FinalCollectionAttemptKey,
RAI_CGROUPSDISP.Group_Disposition,
RAI_CGROUPSDISP.group_disposition_detail
 
   
       from RAI_cgroupsdisp  -- RIGHT OUTER JOIN custsubset ON    --   and

JOIN RAI_custsubset  ON RAI_cgroupsdisp.customerkey = RAI_custsubset.customerkey 
AND
(
--Collection record is within the current group and not equal to the next group
 (RAI_custsubset.collectionkey >= RAI_cgroupsdisp.collectiongroupkey and RAI_custsubset.collectionkey < RAI_cgroupsdisp.nextgroupkey )  
 OR
 --The collection group and next group are the same, and this collection is after it...
 -- Going to try adding equal to it
( RAI_cgroupsdisp.collectiongroupkey = RAI_cgroupsdisp.nextgroupkey and  RAI_custsubset.collectionkey >=RAI_cgroupsdisp.nextgroupkey)
)

select * from RA_cgroupsdisp

select trunc(requestdate,'MONTH'), count(*) from RAI_Final
group by  trunc(requestdate,'MONTH')
order by  trunc(requestdate,'MONTH')

select trunc(requestdate), count(*) from RA_Final
group by  trunc(requestdate)
order by  trunc(requestdate) desc

select * from ra_final
where collectionkey in
(select collectionkey from rai_final)

select * from ods.j2_ispcollection
where ods_current_flag = 1
and requestdate between '01-AUG-2019' and '31-AUG-2019'
and processorgroup not in ('P','D')
and preauthflag not in (1,-1)
and collectionkey not in
(
select collectionkey from ra_final
union
select collectionkey from rai_final
)
order by requestdate
select group_disposition, group_disposition_detail, currencycode, count(*), sum(amount) from ra_final
group by group_disposition, group_disposition_detail, currencycode
order by count(*) desc --group_disposition_detail

select * from RA_custsubset

select 
customerkey,
collectionkey,
    MarketingRegion,
    BillingCountry,
    currencycode,
    amount,
   requestdate,
  -- collectiondate,it's in here but doesn't DO anything.
   processorgroup,
   CARD_TYPE,
   daystocollect,
   declinesuntilsuccessful,
   

 AttemptNumberinGroup,
       --collectionkey, 
collectiongroupkey , 
nextgroupkey,
     --  RA_cgroupsdisp.* --, custsubset.*
COLLECTIONGROUPSTARTDATE,  
COLLECTIONGROUP,
AttemptsInGroup,
FinalCollectionAttemptKey,
from ra_final

select Group_Disposition, count(*), min(collectiongroupstartdate), max(collectiongroupstartdate) from RAI_cgroupsdisp
group by Group_Disposition