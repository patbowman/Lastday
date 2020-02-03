CREATE OR REPLACE PROCEDURE ODS_CENTRAL.ods_retryanalysis_B(

   p_startdate          DATE,
   p_enddate            DATE --,
   --v_rc             IN OUT   TYPES.ref_cursor
)
AS
--   v_fraud_filter   VARCHAR (67);

   v_startdate   DATE; -- was effective
   v_enddate          DATE; -- next_date
   v_ods_date          DATE;



-- PAB 1/9/2012
-- Crystal reports will  ask for a datetime
-- But the query logic assumes just a date
-- If a datetime gets entered, we want to force it to a date
--

BEGIN
   IF p_startdate IS NULL
   THEN
      V_startdate := TRUNC (SYSDATE-1); -- use Yesterday
   ELSE
      V_startdate := TRUNC (p_startdate);
   END IF;

   IF p_enddate IS NULL
   THEN
      V_enddate := TRUNC (SYSDATE)- 1/60/60/24; -- To today
   ELSE
      V_enddate := TRUNC (p_enddate)- 1/60/60/24;
   END IF;







-- And the effective date of the tables needs to be the END of the day, not midnight of.

v_ods_date := v_enddate; -- ...but we figured that in the Enddate, so - 1/60/60/24;

--
-- Get New Attempts
--
-- Table: RA_NewAttempts
----...we should preload all the groups ever, huh.
--This is all that was there before, it's relatively simple and previously checked
-- And takes under 45 minutes to complete.
-- Run it only at the start and it should be a solid check
-- And might be loadable into a useful area.
/*
create table RA_cgroups_test as
select distinct customerkey, requestdate collectiongroupstartdate, collectionkey collectiongroupkey,         
dense_rank() over (partition by customerkey order by collectionkey)        
 collectiongroup,
    CASE 
        when nvl(status,'X') <> 'P' then 0 -- If it's not processed or if it's null, then not collected
        when nvl(response,'X') <> 'A' then 0 -- If it's not Approved or if it's null, then not collected
        else 1 END 
            Collected_Flag,
--And maybe this
     CASE 
     when  nvl(preauthflag,0) in (-1,1) then 'Preauth' 
     when response='C' then 'Cancelled' 
     else null END 
        Exclusion_reason         
from  ods_central.j2_ispcollection co       
where         
processorgroup in ('Z','B','R','O') -- tellyawhat, to start, let's ignore P too        
and retrycount = 0        
and ods_current_flag  = 1    
   -- and customerkey in (select customerkey from custsubset)    
and response not in ('C')     -- ignore cancelled attempts for this purpose - may reconsider.
 -- and co.customerkey in ( 40763324,49464114) 
order by collectionkey;

Could add this
   CASE 
        when nvl(status,'X') <> 'P' then 0 -- If it's not processed or if it's null, then not collected
        when nvl(response,'X') <> 'A' then 0 -- If it's not Approved or if it's null, then not collected
        else 1 END 
            Collected_Flag
And maybe this
     CASE 
     when  nvl(preauthflag,0) in (-1,1) then 'Preauth' 
     when response='C' then 'Cancelled' 
     else null END 
        Exclusion_reason,
select collected_flag,  exclusion_reason, count(*), count(distinct collectiongroupkey) from  RA_cgroups_test
group by collected_flag, exclusion_reason    
insert into  RAD_Collection_Groups 
    select     
    ex1.customerkey, 
    ex1.collectiongroup, --+nvl(prevcollectiongroup,0), 
    ex1.collectiongroupstartdate, 
    ex1.collectiongroupkey, 
   -- ex1.nextgroupkey, 
   -- ex1.nextgroupstartdate,
    CASE when collected_flag = 1 then 1 else 0 END final_status_flag,
    sysdate -- process date time
    from    RA_cgroups ex1
    where exclusion_reason is null
    
    select count(*) from rad_collection_groups
*/
-- set up a script, do it once for the final groups from a year or two back.
--
-- Get New Attempts
--
-- Table: RA_NewAttempts
--
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.rad_newattempts_ex1';
insert into  ods_central.rad_newattempts_ex1 
    select customerkey, collectionkey, response, responsecode, processorgroup, requestdate, 
     typecode Card_Type, 
     currencycode, amount,
     country billingcountry,
     marketingregion,
     RetryCount,
     CASE 
     when  nvl(preauthflag,0) in (-1,1) then 'Preauth' 
     when response='C' then 'Cancelled' 
     else null END 
        Exclusion_reason,
     CASE 
        when retrycount <> 0  then 0 -- non zero retry is not a new group
        when response in ('C') then 0 -- Cancelled records are not a new group
        when processorgroup not in ('Z','B','R','O') then 0 --Only these are new groups, any other is not
        else 1 END 
        New_Group_Flag,
    CASE 
        when nvl(status,'X') <> 'P' then 0 -- If it's not processed or if it's null, then not collected
        when nvl(response,'X') <> 'A' then 0 -- If it's not Approved or if it's null, then not collected
        else 1 END 
            Collected_Flag

    from ods_central.j2_ispcollection co
    LEFT OUTER JOIN ods_central.dw_country 
        ON co.country=DW_COUNTRY.COUNTRYCODE and dw_country.ods_current_flag=1
    where   requestdate >= V_startdate
    and requestdate < V_enddate
     --between '01-JUL-2019' and '02-JUL-2019'
     --and nvl(preauthflag,0) not in (-1, 1) -- no preauths here
      and co.ods_current_flag = 1 order by requestdate;

commit;
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.rad_dispattempts_ex2';
insert into  rad_dispattempts_ex2
select collectionkey, customerkey, response, processorgroup, retrycount, requestdate, amount, ods_current_flag
from ods.j2_ispcollection where 
--ods_current_flag = 1 
--and 
customerkey in (select customerkey from rad_newattempts_ex1)
and requestdate between '01-JAN-2018' and sysdate -- right now, use a year before start date, though that's overkill.
and ods_current_flag =1;
 -- generally should go back a year
commit;

-- Associate New Attempts with Groups 
--
-- RAD_AttemptGroupAssoc_EX2
--
-- Once we have the new attempts, we can associate a group with them.
-- Because (it turns out) there are only 3 possibilities
-- 1) They are the start of a new group, so that is the group they are in
-- And if they are not, they are either
-- 2) The continuation of a group IN the new attempts
-- 3) The continuation of a previously existing group.
--
-- The first two are in the new attempts, the third is in Active groups
-- So look each up as needed as this next step, then move along.
--Previous note/comment
-- aT THIS Point, I know everythin I need to identify the Collection Group Key
    --IF it's a new group, the collection group key is this key
    --IF it's not a new group but an earlier new attempt IS a new group, that's the group key
    --OTHERWISE, look up the latest 
    --(AND at some point could just point it to a generic "Initial successful payment nothing else in group")

EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.RAD_AttemptGroupAssoc_EX2';
insert into  RAD_AttemptGroupAssoc_EX2
with nonnewgroups as (select customerkey, collectionkey from rad_newattempts_ex1 where new_group_flag <> 1),
currentnewgroup as (select n.customerkey, n.collectionkey,
    max(a.collectionkey) 
    -- removed this since it seemed to not change anything AND give multiple results
    OVER (partition by a.customerkey order by a.collectionkey RANGE BETWEEN 0 preceding and 1 following) PrevGoodKey
        from rad_newattempts_ex1 a LEFT OUTER JOIN nonnewgroups n ON a.customerkey = n.customerkey
        where new_group_flag = 1
     --   group by n.customerkey, n.collectionkey
        ),
     --  select * from currentnewgroup where customerkey = 47123559 order by collectionkey
       --select * from rad_newattempts_ex1  collectionkey = 203180107 --customerkey is not null  
       -- order by customerkey, collectionkey
        --new_group_flag <>1
        --where 
 firstnewgroup   as (select customerkey, collectionkey, max(prevgoodkey) RecentGoodKey from currentnewgroup where prevgoodkey < collectionkey group by collectionkey, customerkey ),-- select * from firstnewgroup where  collectionkey = 203180107,   
previousgroup as ( select customerkey, max(a2.collectionGROUPkey) Active_CollectionKey 
    from  RAD_Collection_Groups a2 where final_status_flag =0 group by customerkey
    )

select distinct rad_newattempts_ex1.customerkey,
        rad_newattempts_ex1.collectionkey,
        CASE when New_Group_flag = 1 then rad_newattempts_ex1.collectionkey -- if it's a new group, it's part of this
             when firstnewgroup.customerkey is not null then nvl(RecentGoodKey,-2)
             when previousgroup.customerkey is not null then nvl(Active_CollectionKey,-3) -- these shouldn't ever BE null, but if they are we'll know whence they came
             else -1
             END collectiongroupkey --,
         --    CASE when New_Group_flag = 1 then 'New' --rad_newattempts_ex1.collectionkey -- if it's a new group, it's part of this
          --   when firstnewgroup.customerkey is not null then 'In New Group'-- nvl(PrevGoodKey,-2)
           -- when previousgroup.customerkey is not null then 'Previous group'-- nvl(Active_CollectionKey,-3) -- these shouldn't ever BE null, but if they are we'll know whence they came
            -- else 'X'
            -- END
        -- 'X'
          --collectiongroupselect
 from rad_newattempts_ex1
 LEFT OUTER JOIN firstnewgroup on rad_newattempts_ex1.collectionkey = firstnewgroup.collectionkey
 LEFT OUTER JOIN previousgroup ON rad_newattempts_ex1.customerkey = previousgroup.customerkey;
 --group by customerkey, collectionkey
 --)
 commit;
 --select * from thing
 --where collectionkey in (select collectionkey from thing group by collectionkey having count(*) > 1)
  --select count(*) from rad_newattempts_ex1
 --select collectiongroupkey, max(collectionkey) from thing where collectiongroupkey < 0
--group by collectiongroupkey 203280051
-- So its working, just can't do anything with earlier ones
-- And may want to just say "-1 means it's an earlier one, i couldn't find it.
--select max(collectiongroupkey) from RAD_Active_Collection_Groups 203280036


--
-- Get stats for new records - EX3
--

EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.rad_newattempts_stats_ex3';
insert into  rad_newattempts_stats_ex3  
select  ex2.collectiongroupkey, 
        count(ex2.collectionkey) NewAttempts, 
        max(ex2.collectionkey) MostRecentAttempt
from RAD_AttemptGroupAssoc_EX2 ex2
 group by ex2.collectiongroupkey;

commit;


--
--
-- Get Active (already existing) Groups
--
-- Table:rad_active_groups_ex1
--
-- HEY -- Want to change the sydate here eventually
-- It will close out items incorrectly if you are running old data
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.rad_active_groups_ex1';
insert into rad_active_groups_ex1
select distinct   ex1.CUSTOMERKEY,
  ex1.COLLECTIONGROUP           ,
  ex1.COLLECTIONGROUPSTARTDATE ,
  ex1.COLLECTIONGROUPKEY        ,
  ex1.NEXTGROUPKEY              ,
  ex1.NEXTGROUPSTARTDATE        ,
  ex1.FINAL_STATUS_FLAG         ,
    c.status --, c.enddate, c.cancelcode 
from rad_collection_groups ex1
LEFT OUTER JOIN ods.j2_ispcustomerchange c ON ex1.customerkey=c.customerkey
where sysdate between ods_effective_from and ods_effective_to -- for index more than need
and final_status_flag = 0;
commit;
--
-- New Groups: Create New Group Records
--
-- Table: RAD_New_Collection_Groups
-- select * from rad_new_collection_groups order by customerkey, collectiongroupkey
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.RAD_New_Collection_Groups'; --to be _EX4
insert into RAD_New_Collection_Groups
    select customerkey, 
    dense_rank() over (partition by customerkey order by collectionkey) CollectionGroup,
    RequestDate CollectionGroupStartDate,
    collectionkey CollectionGroupKey 
    from rad_newattempts_ex1
    where new_group_flag = 1;
 commit;
 
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.RAD_New_Collection_Groups_EX1';
insert into   RAD_New_Collection_Groups_EX1
    select customerkey, 
    CollectionGroup,
    CollectionGroupStartDate,
    CollectionGroupKey,
    max(collectiongroupkey) OVER
     (partition by customerkey order by collectiongroup RANGE BETWEEN 0 preceding and 1 following)  
        NextGroupKey,      
    max(CollectionGroupStartDate) OVER 
     (partition by customerkey order by CollectionGroupKey RANGE BETWEEN 0 preceding and 1 following)  
        NextGroupStartDate
    from RAD_New_Collection_Groups;
 commit;
 
 
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.RAD_New_Collection_Groups_EX3';
insert into RAD_New_Collection_Groups_EX3
--as
--with RAD_Collection_groups as
--(
--select * from RAD_Active_Collection_Groups 
--union all -- shouldn't be any duplicates
--select * from RAD_Final_Collection_Groups 
--)
select RAD_Collection_Groups.customerkey, max(RAD_Collection_Groups.collectiongroup) prevcollectiongroup from RAD_Collection_Groups
JOIN RAD_New_Collection_Groups_EX1 ON RAD_New_Collection_Groups_EX1.customerkey = RAD_Collection_Groups.customerkey
group by RAD_Collection_Groups.customerkey having max(RAD_Collection_Groups.collectiongroup) > 1;

commit;
-- Before we do the insert,
-- We're going to want to update existing nextgroup key

-- New Groups: Finalize/Update Existing Previous Group Records 
-- (where applicable)
-- Table: RAD_Active_Collection_Groups

-- Final is FinalIZED, not last one
-- Might have to work on that.
-- Anyway, if there's a final one


insert into  RAD_Collection_Groups 
    select     
    ex1.customerkey, 
    ex1.collectiongroup +nvl(prevcollectiongroup,0), 
    ex1.collectiongroupstartdate, 
    ex1.collectiongroupkey, 
    ex1.nextgroupkey, 
    ex1.nextgroupstartdate,
    CASE when collected_flag = 1 then 1 else 0 END final_status_flag,
    sysdate -- process date time
    from RAD_New_Collection_Groups_EX1 ex1
        JOIN rad_newattempts_ex1 on rad_newattempts_ex1.collectionkey=ex1.collectiongroupkey
        LEFT OUTER JOIN  RAD_New_Collection_Groups_EX3 ex3 ON ex3.customerkey = ex1.customerkey;
commit;


--
-- Not New Groups: Update Existing Group Records
-- Table: RAD_Active_Collection_Groups
-- 
EXECUTE IMMEDIATE 'truncate table ODS_CENTRAL.RAD_New_Group_Stats' ;
 insert into RAD_New_Group_Stats
    select distinct  co.customerkey, 
    cg.collectiongroup, cg.collectiongroupstartdate, cg.collectiongroupkey, 
    cg.nextgroupkey, cg.nextgroupstartdate,
    ex3.NewAttempts,
    ex3.MostRecentAttempt,
   -- co.exclusion_reason,
    co.collected_flag,
    co.new_group_flag
    --count(collectionkey) OVER (partition by co.customerkey, collectiongroup) AttemptsInGroup,        
    --max(collectionkey) OVER (partition by RAD_Collection_Groups.customerkey, collectiongroup) FinalAttempt 
    from rad_newattempts_ex1 co
    JOIN RAD_AttemptGroupAssoc_EX2 ex2 ON EX2.COLLECTIONKEY= CO.COLLECTIONKEY 
    JOIN rad_newattempts_stats_ex3  ex3 ON EX2.COLLECTIONGROUPKEY= ex3.collectiongroupkey
    JOIN  RAD_Collection_Groups cg    ON cg.collectiongroupkey = ex2.collectiongroupkey
    
    --JOIN ods_central.j2_ISPCOLLECTION co
    -- ON RAD_Collection_Groups.customerkey = co.customerkey 
      --  and collectionkey >= collectiongroupkey         
       -- and (collectionkey < nextgroupkey or collectiongroupkey = nextgroupkey) -- include through the next group, or include any if it's the last one        
    where  exclusion_reason is null;
commit;


--
-- Update finalized Actives to Final
-- Note that this is the GROUP update, not the DISPOSITION update
-- (Not sure yet if it needs to be moved to later, anyway, but want to note it)
--
-- Moved to later, but later is only one step.
-- Doesn't seem to make sense to have it after Final, or before Final,
-- or before DISP.Unless I'm figuring it out along the way.
-- Get Finalized actives - 3 ways
    -- 1. This group is collected
    -- 2. New group has started
    -- 3. Customer has cancelled.
 -- Option 1: If next group key is already identified, it's final    

 UPDATE RAD_COLLECTION_GROUPS
 set FINAL_STATUS_FLAG = 1
 where final_status_flag = 0
 -- greater than is technically correct,
 --but really as long as next group key has been set, it's done.
 and nextgroupkey <> collectiongroupkey;
 commit;

-- Option 1a: If there is a new group...well, it's probably already in there but update it. 
 UPDATE RAD_COLLECTION_GROUPS
 set FINAL_STATUS_FLAG = 1
 where final_status_flag = 0
 and exists 
 (
  with newgroups as
(select rad_newattempts_ex1.customerkey, rad_newattempts_ex1.collectionkey, rad_newattempts_ex1.new_group_flag, collected_flag, exclusion_reason, collectiongroupkey  collectiongroupkey
     from rad_newattempts_ex1
    LEFT OUTER JOIN RAD_AttemptGroupAssoc_EX2 ex2 ON ex2.collectionkey = rad_newattempts_ex1.collectionkey
   where exclusion_reason is null
    )
  select 1 from newgroups


 /* index(RAD_Collection_Groups)*/ --newgroups

    --JOIN RAD_New_Collection_Groups_EX3 ex3 ON ex3. 
    --JOIN rad_newattempts_ex1 ON 
  --  select * from RAD_Collection_Groups
    --LEFT OUTER JOIN RAD_Collection_Groups on 
    where 
    newgroups.customerkey = RAD_Collection_Groups.customerkey -- Same customer
    and  newgroups.collectionkey > RAD_Collection_Groups.collectiongroupkey -- Newer group key
    and final_status_flag =0 -- Previous group isn't updated
    and   new_group_flag = 1 -- And this is a new group
--    order by newgroups.customerkey, newgroups.collectionkey
);
commit;
-- Option 2 -- If the group has been collected, set final to 1
-- (now item 3a for update process
-- may later change it to use the Disp calculation directly.
 UPDATE RAD_COLLECTION_GROUPS
 set FINAL_STATUS_FLAG = 1
 where final_status_flag = 0
 and exists 
 (
 with newgroups as
(select rad_newattempts_ex1.customerkey, rad_newattempts_ex1.collectionkey, rad_newattempts_ex1.new_group_flag, collected_flag, exclusion_reason, collectiongroupkey  collectiongroupkey
     from rad_newattempts_ex1
    LEFT OUTER JOIN RAD_AttemptGroupAssoc_EX2 ex2 ON ex2.collectionkey = rad_newattempts_ex1.collectionkey
   where exclusion_reason is null
    )
   select 1 
--select*

 /* index(RAD_Collection_Groups)*/ from newgroups --, rad_collection_groups

    --JOIN RAD_New_Collection_Groups_EX3 ex3 ON ex3. 
    --JOIN rad_newattempts_ex1 ON 
  --  select * from RAD_Collection_Groups
    --LEFT OUTER JOIN RAD_Collection_Groups on 
    where 
    newgroups.collectiongroupkey = RAD_Collection_Groups.collectiongroupkey
    and final_status_flag =0 
    and collected_flag = 1
    --order by newgroups.customerkey, newgroups.collectionkey    
   );
commit;


--select * from rad_collection_groups
-- Thinking that I may need to 

-- And Thinking that maybe this needs to be for the New attempts, not new groups


---ARGHHHHGLK
-- We're not figuring final attempt after the first setup.
-- So it's not getting duped because later attempts aren't going in.
--
--
-- Determine Dispositions and Update Groups
-- Table: RAD_Active_Collection_Groups
-- RAD_Collection_Group_Disp
-- Figure updated and new dispositions
-- Now item 2 in list of five post-grouping steps
EXECUTE IMMEDIATE 'truncate table ods_central.RAD_Collection_Group_Disp_CF1';
 INSERT into rad_collection_group_disp_CF1
    Select customerkey, collectiongroup, collectiongroupstartdate, collectiongroupkey, nextgroupkey, nextgroupstartdate, newattempts, mostrecentattempt, 
    0,0, 'Paid','Paid on first Attempt', 1, sysdate
    from Rad_new_group_stats
        Where new_group_flag =1 and collected_flag = 1;
commit;
--select * from rad_new_group_stats
--select * from RAD_Collection_Group_Disp_CF1

insert into RAD_Collection_Group_Disp_CF1
--create table   RAD_Collection_Group_Disp_CF1 as select * from RAD_Collection_Group_Disp
(select distinct  RAD_New_Group_Stats.customerkey, RAD_New_Group_Stats.collectiongroup, RAD_New_Group_Stats.collectiongroupstartdate, RAD_New_Group_Stats.collectiongroupkey, RAD_New_Group_Stats.nextgroupkey, RAD_New_Group_Stats.nextgroupstartdate, RAD_New_Group_Stats.newattempts, RAD_New_Group_Stats.mostrecentattempt,
--RAD_New_Group_Stats. 
--RAD_New_Group_Stats.*,    
--are these group attributes or attempt attributes?
--(I think they're being requested for attempts but actually they are group)
CASE 
    when DISP.RESPONSE = 'D' then null 
    else trunc( disp.requestdate)-collectiongroupstartdate END
        daystocollect,
 CASE   
    when DISP.RESPONSE = 'D' then null 
    else      newattempts-1 END
       declinesuntilsuccessful,   
CASE when DISP.RESPONSE='A'  then 'Paid'        
    when DISP.RESPONSE='D' and retrycount < 4 and collectiongroup = max(collectiongroup) OVER (partition by RAD_New_Group_Stats.customerkey) then 'Declining'        
    else 'Failed' END 
        Group_Disposition,        
CASE when DISP.RESPONSE='A' and disp.retrycount =0 and disp.processorgroup in ('B','Z') then 'Paid on first attempt'        
    when DISP.RESPONSE='A' and disp.retrycount > 0 and disp.processorgroup in ('B','Z') then 'Paid on batch try '||to_char(disp.retrycount+1)        
    when DISP.RESPONSE='A' and disp.processorgroup='I' then 'Paid on info update try '||to_char(disp.retrycount+1)        
    when DISP.RESPONSE='A' and disp.processorgroup='C' then 'Paid with CS help try '||to_char(disp.retrycount+1)        
    when DISP.RESPONSE='A' and disp.processorgroup='R' then 'Paid Realtime try '||to_char(disp.retrycount+1)        
    when DISP.RESPONSE='A' and disp.processorgroup='O' then 'Paid Topup Process try '||to_char(disp.retrycount+1)        
    when DISP.RESPONSE='A' then 'Paid non-batch try '||to_char(disp.retrycount+1)        
    when DISP.RESPONSE='D' and retrycount < 4 and collectiongroup = max(collectiongroup) OVER (partition by RAD_New_Group_Stats.customerkey) then 'Declining through try '||to_char(retrycount+1) || ' (Attempt in group  TBD)'      
    else 'Failed' END 
        Group_Disposition_Detail,
        0, -- Final_Status_flag, will handle later
        sysdate -- Process_Datetime        
from   RAD_New_Group_Stats   
JOIN rad_dispattempts_ex2 disp 
    ON RAD_New_Group_Stats.customerkey = disp.customerkey 
    --select * from RAD_New_Group_Stats
    --No, flipping back:Dispostion IS all about the last one here:
    -- I'm not looking at anything or counting anything exception what the most recent attempt did
     and collectionkey =mostrecentattempt
--and collectionkey between collectiongroupkey and mostrecentattempt
--and requestdate between collectiongroupstartdate and nextgroupstartdate -->= collectiongroupkey and collectionkey < nextgroupkey    
       -- and collectionkey=finalattempt-->= collectiongroupkey and collectionkey < nextgroupkey        
--JOIN rad_newattempts_ex1 on rad_newattempts_ex1.collectionkey =     disp.collectionkey
--where exclusion_reason is not null
and disp.ods_current_flag = 1
and not (new_group_flag =1 and collected_flag = 1)
);
commit;

-- UPDATE any active groups based on disposition
-- 
/*
BUT this doesn't work as expected SO using Merg
UPDATE RAD_Collection_Group_Disp
SET RAD_COLLECTION_GROUP_DISP.nextgroupkey = RAD_Collection_Group_Disp_CF1.NextGroupKey 
--select * from RAD_Collection_Group_Disp_CF1,RAD_Collection_Group_Disp
WHERE exists
(select 1 from RAD_Collection_Group_Disp_CF1.collectiongroupkey = RAD_Collection_Group_Disp.COLLECTIONGROUPKEY
and   RAD_Collection_Group_Disp.nextgroupkey < RAD_Collection_Group_Disp_CF1.NextGroupKey
*/
-- There are 6 possible updates,
-- 4 of them are 2 groups of 2, so 4 MERGE in total

MERGE INTO RAD_Collection_Group_Disp d
    USING (select * from RAD_Collection_Group_Disp_CF1) cf1
    ON (cf1.collectiongroupkey=d.collectiongroupkey)
    when MATCHED then UPDATE 
                set d.nextgroupkey = cf1.NextGroupKey, 
                    d.nextgroupstartdate = cf1.NextGroupstartdate  
    where d.nextgroupkey < cf1.NextGroupKey;
commit;
    
MERGE INTO RAD_Collection_Group_Disp d
    USING (select * from RAD_Collection_Group_Disp_CF1) cf1
    ON (cf1.collectiongroupkey=d.collectiongroupkey)
    when MATCHED then UPDATE 
                set d.attemptsingroup = cf1.attemptsingroup 
    where d.attemptsingroup < cf1.attemptsingroup;
commit;
  
MERGE INTO RAD_Collection_Group_Disp d
    USING (select * from RAD_Collection_Group_Disp_CF1) cf1
    ON (cf1.collectiongroupkey=d.collectiongroupkey)
    when MATCHED then UPDATE 
                set d.finalattempt = cf1.finalattempt 
    where d.finalattempt < cf1.finalattempt;
commit;
  
MERGE INTO RAD_Collection_Group_Disp d
    USING (select * from RAD_Collection_Group_Disp_CF1) cf1
    ON (cf1.collectiongroupkey=d.collectiongroupkey)
    when MATCHED then UPDATE 
                set d.DaysToCollect = cf1.DaysToCollect, 
                    d.DeclinesUntilSuccessful = cf1.DeclinesUntilSuccessful  
    where  nvl(cf1.DaysToCollect,0) > 0;       -- update if not zero
commit;
  
--
-- Oh, right, the dispositions themselves
MERGE INTO RAD_Collection_Group_Disp d
    USING (select * from RAD_Collection_Group_Disp_CF1) cf1
    ON (cf1.collectiongroupkey=d.collectiongroupkey)
    when MATCHED then UPDATE 
                set d.group_disposition = cf1.group_disposition, 
                    d.group_disposition_detail = cf1.group_disposition_detail  
    where d.group_disposition_detail <> cf1.group_disposition_detail;
commit;
--INSERT new groups
-- Yeah I could probably do it from the Merge but it's my first time, though.

insert into RAD_Collection_Group_Disp -- just use this for now, eventually will flip it
select cf1.* from RAD_Collection_Group_Disp_CF1 cf1
JOIN RAD_AttemptGroupAssoc_EX2 ON RAD_AttemptGroupAssoc_EX2.collectiongroupkey = CF1.collectiongroupkey
JOIN rad_newattempts_ex1  ON rad_newattempts_ex1.collectionkey = RAD_AttemptGroupAssoc_EX2.collectionkey

where new_group_flag = 1; -- need to add this!
commit;
--
-- Option 3: If the customer has cancelled
-- 
--(Now Update 5 once groups are set up)
-- Which shouldn't be a problem, get the customer records and move along, but no..
--   
     UPDATE RAD_COLLECTION_GROUPS
 set FINAL_STATUS_FLAG = 1--,--,
    --group_disposition = 'Failed',
    --group_disposition_detail = 'Customer cancelled'
 where final_status_flag = 0
 and exists
    (select 1 from rad_active_groups_ex1 ex1--,rad_collection_groups
    where ex1.collectiongroupkey = rad_collection_groups.collectiongroupkey
    and status = 'I');
    /* -- NEED FINAL STATUS FLAG to do this.
      UPDATE     RAD_Collection_Group_Disp
 set FINAL_STATUS_FLAG = 1,--,
    group_disposition = 'Failed',
    group_disposition_detail = 'Customer cancelled'
 where final_status_flag = 0
 and exists
    (select 1 from rad_active_groups_ex1 ex1--,rad_collection_groups
    where ex1.collectiongroupkey =     RAD_Collection_Group_Disp.collectiongroupkey
    and status = 'I');   
*/
    commit;
--select * from RAD_New_Collection_Groups_EX3
 
 --create table RAD_Final as 

insert into RAD_Final
    select
   rad_newattempts_ex1.customerkey,
   rad_newattempts_ex1.collectionkey,
    MarketingRegion,
    BillingCountry,
    currencycode,
    amount,
   requestdate,
   processorgroup,
   CARD_TYPE,
   daystocollect,
   declinesuntilsuccessful,
dense_rank() over (partition by RAD_Collection_Group_Disp.collectiongroupkey order by rad_newattempts_ex1.collectionkey) AttemptNumberinGroup, 
RAD_Collection_Group_Disp.collectiongroupkey , RAD_Collection_Group_Disp.nextgroupkey,
     --  RA_cgroupsdisp.* --, custsubset.*
RAD_Collection_Group_Disp.COLLECTIONGROUPSTARTDATE,  
RAD_Collection_Group_Disp.COLLECTIONGROUP,
RAD_Collection_Group_Disp.AttemptsInGroup,
RAD_Collection_Group_Disp.FinalAttempt FinalCollectionAttemptKey,
RAD_Collection_Group_Disp.Group_Disposition,
RAD_Collection_Group_Disp.group_disposition_detail,
RAD_Collection_Group_Disp.final_status_flag,
sysdate -- processdatetime
 
   
from RAD_Collection_Group_Disp  -- RIGHT OUTER JOIN custsubset ON    --   and
JOIN RAD_AttemptGroupAssoc_EX2 ON RAD_AttemptGroupAssoc_EX2.collectiongroupkey = RAD_collection_group_Disp.collectiongroupkey
JOIN rad_newattempts_ex1  ON rad_newattempts_ex1.collectionkey = RAD_AttemptGroupAssoc_EX2.collectionkey
--JOIN 
--AND
--(
--Collection record is within the current group and not equal to the next group
 --(rad_newattempts_ex1.collectionkey >= RAD_Collection_Group_Disp.collectiongroupkey and rad_newattempts_ex1.collectionkey < RAD_Collection_Group_Disp.nextgroupkey )  
 -- Removed this FOR NOW, only because I don't have collection and group linked up elsewhere.
 --OR
 --The collection group and next group are the same, and this collection is after it...
 -- Going to try adding equal to it
--( rad_newattempts_ex1.collectiongroupkey = RAD_Collection_Group_Disp .nextgroupkey and  rad_newattempts_ex1.collectionkey >=RAD_Collection_Group_Disp.nextgroupkey)
--
--) --select * from RAD_AttemptGroupAssoc_EX2

;
commit;

/*
create table RAD_Final_Collection_Groups as
select * from ra_nextcgroups  
where ra_nextcgroups.customerkey < 1000
create table RAD_Active_Collection_Groups as
select * from ra_nextcgroups  --JOIN  ra_nextcgroups ON RA_CGROUPS.COLLECTIONGROUPKEY=RA_NEXTCGROUPS.COLLECTIONGROUPKEY
where ra_nextcgroups.customerkey < 1000
*/
--
-- Identify Final Dispositions
-- (To start, either Collected or New Group - and new group may have been previous)
-- Table: RAD_Active_Dispositions
-- 2 monts, 6:52 minutes. That should be fine.



/*
insert into  RAD_Final_Collection_Groups 
    select 
    ex1.customerkey, 
    ex1.collectiongroup+nvl(prevcollectiongroup,0), 
    ex1.collectiongroupstartdate, 
    ex1.collectiongroupkey, 
    ex1.nextgroupkey, 
    ex1.nextgroupstartdate
    from RAD_New_Collection_Groups_EX1 ex1
    JOIN rad_newattempts_ex1 on rad_newattempts_ex1.collectionkey=ex1.collectiongroupkey
    LEFT OUTER JOIN  RAD_New_Collection_Groups_EX3 ex3 ON ex3.customerkey = ex1.customerkey
    and collected_flag = 1;
commit;
insert into  RAD_Active_Collection_Groups 
    select     
    ex1.customerkey, 
    ex1.collectiongroup +nvl(prevcollectiongroup,0), 
    ex1.collectiongroupstartdate, 
    ex1.collectiongroupkey, 
    ex1.nextgroupkey, 
    ex1.nextgroupstartdate from RAD_New_Collection_Groups_EX1 ex1
    JOIN rad_newattempts_ex1 on rad_newattempts_ex1.collectionkey=ex1.collectiongroupkey
    LEFT OUTER JOIN  RAD_New_Collection_Groups_EX3 ex3 ON ex3.customerkey = ex1.customerkey
    and collected_flag <> 1;
commit;
*/
-- April to October: 23 minutes, 12.8M rows
-- Just october 10-23...52 minutes, 32.7 rows...something is wrong.
-- It's starting at New Stats
--Feb to October: 1:05 8.8M rows
--February to May 1:19:59...so a bit slower than before
-- and none of the merges...merged.
-- current fversion took 8+ hours to not finish February to June
-- Not necessarily a huge problem, but a little concerning
--select * from rad_final
--13 days, 27:55 minutes, most in Disp. 
--Huh -- new setup takes a very consistent 10 minutes for 2 ays, 4 days, and 10 days.
-- Faster for a single day but still.


END;
/