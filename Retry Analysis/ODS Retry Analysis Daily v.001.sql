--
-- Get New Attempts
--
-- Table: RA_NewAttempts
--select * from RAD_Active_Collection_Groups
--select * from RAD_NewAttempts
--truncate table pbowman.RAD_NewAttempts 
-- drop table pbowman.rad_newattempts
insert into  pbowman.RAD_NewAttempts 

select customerkey, collectionkey, response, responsecode, processorgroup, requestdate, 
 --collectiondate,
 typecode Card_Type, 
 currencycode, amount,
 country billingcountry,marketingregion,
 RetryCount,
 CASE when  nvl(preauthflag,0) in (-1,1) then 'Preauth' when response='C' then 'Cancelled' else null END Exclusion_reason,
 CASE 
    when retrycount <> 0  then 0 -- non zero retry is not a new group
    when response in ('C') then 0 -- Cancelled records are not a new group
    when processorgroup not in ('Z','B','R','O') then 0 --Only these are new groups, any other is not
    else 1 END New_Group_Flag,
CASE when nvl(status,'X') <> 'P' then 0 -- If it's not processed or if it's null, then not collected
    when nvl(response,'X') <> 'A' then 0 -- If it's not Approved or if it's null, then not collected
    else 1 END Collected_Flag --,
    --dense_rank() over (partition by customerkey order by collectionkey)    CollectionGroup_Net Maybe but probably not since it'll count non-groups
-- ods_central.j2_ispcollection co 
 --co.*
  from ods_central.j2_ispcollection co
LEFT OUTER JOIN ods_central.dw_country ON co.country=DW_COUNTRY.COUNTRYCODE and dw_country.ods_current_flag=1
  where   requestdate between '09-JUN-2001' and '10-JUN-2001'
 --between '01-JUL-2019' and '02-JUL-2019'
 --and nvl(preauthflag,0) not in (-1, 1) -- no preauths here
  and co.ods_current_flag = 1 order by requestdate ;
commit;

--
-- Identify Which Attempts are New Groups
-- 
--[Update RA_NewAttempts with New flag]
--select count(*) from RAD_NewAttempts where new_group_flag = 0

-- Note that some of these are NOT new group flag BUT have no previous ones....
--
-- Split Attempts by New Group/Not
--
--May not be much here...

--
-- New Groups: Create New Group Records
--
-- Table: RAD_New_Collection_Groups
-- truncate table rad_new_collection_groups
-- select * from rad_new_collection_groups order by customerkey, collectiongroupkey
--create table RAD_New_Collection_EX1
insert into RAD_New_Collection_Groups
select customerkey, 
dense_rank() over (partition by customerkey order by collectionkey)    CollectionGroup,
RequestDate CollectionGroupStartDate,
collectionkey CollectionGroupKey --,
--max(collectionkey) OVER (partition by customerkey order by collectionkey RANGE BETWEEN 0 preceding and 1 following)  NextGroupKey,        
--max(requestdate) OVER (partition by customerkey order by collectionkey RANGE BETWEEN 0 preceding and 1 following)  NextGroupStartDate   
--null NextGroupKey,
--null NextGroupStartDate
 from rad_newattempts
where new_group_flag = 1
/*
create table RAD_New_Collection_Groups_EX1 as
select * from rad_new_collection_groups
truncate table RAD_New_Collection_Groups_EX1
*/
insert into  RAD_New_Collection_Groups_EX1
select customerkey, 
  CollectionGroup,
CollectionGroupStartDate,
 CollectionGroupKey,
  max(collectiongroupkey) OVER (partition by customerkey order by collectiongroup RANGE BETWEEN 0 preceding and 1 following)  NextGroupKey,      
max(CollectionGroupStartDate) OVER (partition by customerkey order by CollectionGroupKey RANGE BETWEEN 0 preceding and 1 following)  NextGroupStartDate   
--null NextGroupKey,
--null NextGroupStartDate
from RAD_New_Collection_Groups;
 commit;
 
 
/* select * from RAD_New_Collection_Groups_EX1 order by customerkey, collectiongroup
 update RAD_New_Collection_Groups_EX1
 set nextgroupstartdate = (select  max(collectiongroupstartdate) from RAD_New_Collection_Groups_EX1 where RAD_New_Collection_Groups_EX1.collectiongroupkey=RAD_New_Collection_Groups_EX1.nextgroupkey)
--update rad_new_collection_groups
--set CollectionGroup = CollectionGroup Plus 1 --(select max(nvl(CollectionGroup,0))+1 from rad_new_collection_groups x where x.customerkey = rad_new_collection_groups.customerkey)
update RAD_New_Collection_Groups_EX1

set collectiongroup = nvl(collectiongroup,0)+nvl((select max(nvl(collectiongroup,0)) CollectionGroupAdd from RAD_Final_Collection_Groups where RAD_Final_Collection_Groups.customerkey=customerkey),0)
*/
/*
update rad_new_collection_groups
set NextGroupKey = (select max(collectiongroupkey) OVER (order by collectiongroup RANGE BETWEEN 0 preceding and 1 following) from rad_new_collection_groups x  where x.customerkey =rad_new_collection_groups.customerkey );
update rad_new_collection_groups
set NextGroupStartDate = CollectionGroupStartDate;
select count(*) from rad_new_collection_groups
*/
-- Before we do the insert,
-- We're going to want to update existing nextgroup key


-- Final is FinalIZED, not last one
-- Might have to work on that.
-- Anyway, if there's a final one
insert into RAD_Final_Collection_Groups 
select ex1.* from RAD_New_Collection_Groups_EX1 ex1
JOIN rad_newattempts on rad_newattempts.collectionkey=ex1.collectiongroupkey
and collected_flag = 1

insert into RAD_Active_Collection_Groups 
select ex1.* from RAD_New_Collection_Groups_EX1 ex1
JOIN rad_newattempts on rad_newattempts.collectionkey=ex1.collectiongroupkey
and collected_flag <> 1

--select* from RAD_Final_Collection_Groups 
--where 
select * from rad_new_collection_groups
create table RAD_New_Collection_Groups as
select * from ra_nextcgroups  --JOIN  ra_nextcgroups ON RA_CGROUPS.COLLECTIONGROUPKEY=RA_NEXTCGROUPS.COLLECTIONGROUPKEY
where ra_nextcgroups.customerkey < 1000

select nvl(collectiongroup,0) CollectionGroupAdd from RAD_Final_Collection_Groups
-- select count(*) from rad_new_collection_groups group by customerkey having count(*)  > 1
-- New Groups: Finalize/Update Existing Previous Group Records 
-- (where applicable)
-- Table: RAD_Active_Collection_Groups

--
-- Not New Groups: Update Existing Group Records
-- Table: RAD_Active_Collection_Groups

--create table RAD_New_Attempt_Stats as        
--(
insert into RAD_New_Group_Stats
select distinct  co.customerkey, collectiongroup, collectiongroupstartdate, collectiongroupkey, nextgroupkey, nextgroupstartdate,
        
count(collectionkey) OVER (partition by co.customerkey, collectiongroup) AttemptsInGroup,        
max(collectionkey) OVER (partition by RAD_Final_Collection_Groups.customerkey, collectiongroup) FinalAttempt 
from RAD_NewAttempts   co
JOIN  RAD_Final_Collection_Groups     
--JOIN ods_central.j2_ISPCOLLECTION co
 ON RAD_Final_Collection_Groups.customerkey = co.customerkey and collectionkey >= collectiongroupkey         
       and (collectionkey < nextgroupkey or collectiongroupkey = nextgroupkey) -- include through the next group, or include any if it's the last one        
        
where  exclusion_reason is null;       
--ods_current_flag  = 1 and
--and response not in ('C')       
--and nvl(preauthflag,0) not in (-1, 1)   -- don't count pre-auths for this 
--);

commit;
--
-- Determine Dispositions and Update Groups
-- Table: RAD_Active_Collection_Groups
create table RAD_Collection_Group_Disp as  
insert into   RAD_Collection_Group_Disp    
(select distinct  RAD_New_Group_Stats.*,    
--are these group attributes or attempt attributes?
--(I think they're being requested for attempts but actually they are group)
CASE when DISP.RESPONSE = 'D' then null else     trunc( disp.requestdate)-collectiongroupstartdate END
 daystocollect,
 CASE when DISP.RESPONSE = 'D' then null else      attemptsingroup-1 END
       declinesuntilsuccessful ,   
CASE when DISP.RESPONSE='A'  then 'Paid'        
when DISP.RESPONSE='D' and retrycount < 4 and collectiongroup = max(collectiongroup) OVER (partition by RAD_New_Group_Stats.customerkey) then 'Declining'        
else 'Failed' END Group_Disposition,        
CASE when DISP.RESPONSE='A' and disp.retrycount =0 and disp.processorgroup in ('B','Z') then 'Paid on first attempt'        
when DISP.RESPONSE='A' and disp.retrycount > 0 and disp.processorgroup in ('B','Z') then 'Paid on batch try '||to_char(disp.retrycount+1)        
when DISP.RESPONSE='A' and disp.processorgroup='I' then 'Paid on info update try '||to_char(disp.retrycount+1)        
when DISP.RESPONSE='A' and disp.processorgroup='C' then 'Paid with CS help try '||to_char(disp.retrycount+1)        
when DISP.RESPONSE='A' and disp.processorgroup='R' then 'Paid Realtime try '||to_char(disp.retrycount+1)        
when DISP.RESPONSE='A' and disp.processorgroup='O' then 'Paid Topup Process try '||to_char(disp.retrycount+1)        
when DISP.RESPONSE='A' then 'Paid non-batch try '||to_char(disp.retrycount+1)        
when DISP.RESPONSE='D' and retrycount < 4 and collectiongroup = max(collectiongroup) OVER (partition by RAD_New_Group_Stats.customerkey) then 'Declining through try '||to_char(retrycount+1) || ' (Attempt in group  TBD)'      
else 'Failed' END Group_Disposition_Detail        
 --, max(collectionkey) OVER (partition by cgroups2.customerkey, collectiongroup) FinalAttempt         
from  RAD_New_Group_Stats   
  
JOIN ods_central.j2_ISPCOLLECTION disp ON RAD_New_Group_Stats.customerkey = disp.customerkey and collectionkey=finalattempt-->= collectiongroupkey and collectionkey < nextgroupkey        
--JOIN RAD_NewAttempts on RAD_NewAttempts.collectionkey =     disp.collectionkey
--where exclusion_reason is not null
       
);
commit;





create table RAD_Final_Collection_Groups as
select * from ra_nextcgroups  
where ra_nextcgroups.customerkey < 1000
create table RAD_Active_Collection_Groups as
select * from ra_nextcgroups  --JOIN  ra_nextcgroups ON RA_CGROUPS.COLLECTIONGROUPKEY=RA_NEXTCGROUPS.COLLECTIONGROUPKEY
where ra_nextcgroups.customerkey < 1000
--
-- Identify Final Dispositions
-- (To start, either Collected or New Group - and new group may have been previous)
-- Table: RAD_Active_Dispositions
create table RAD_Active_Dispositions as
select * from ra_final
where customerkey < 1000
-- 
-- Move Finalized Dispositions to Final Area
--Table: RAD_Final_Dispositions
create table RAD_Final_Dispositions as
select * from ra_final 
where customerkey < 1000