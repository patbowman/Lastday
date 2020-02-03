--
-- Get New Attempts
--
-- Table: RA_NewAttempts
--
truncate table rad_newattempts_ex1;
insert into  pbowman.rad_newattempts_ex1 
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
    where   requestdate >= '01-JAN-2018' 
    and requestdate < '02-JAN-2018'
     --between '01-JUL-2019' and '02-JUL-2019'
     --and nvl(preauthflag,0) not in (-1, 1) -- no preauths here
      and co.ods_current_flag = 1 order by requestdate;

commit;
--
-- RAD_AttemptGroupAssoc_EX2
--
-- Once we have the new attempts, we can associate a group with them.
-- Because (it turns out) there are only 3 possibilities
-- 1) They are the start of a new group, so that is the group they are in
-- And if they are not, they are either
-- 2) The continuatino of a group IN the new attempts
-- 3) The  continuation of a previously existing group.
--
-- The first two are in the new attempts, the third is in Active groups
-- So look each up as needed as this next step, then move along.
--Previous note/comment
-- aT THIS Point, I know everythin I need to identify the Collection Group Key
    --IF it's a new group, the collection group key is this key
    --IF it's not a new group but an earlier new attempt IS a new group, that's the group key
    --OTHERWISE, look up the latest 
    --(AND at some point could just point it to a generic "Initial successful payment nothing else in group")

truncate table RAD_AttemptGroupAssoc_EX2;
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
    from  RAD_Active_Collection_Groups a2 group by customerkey
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
-- New Groups: Create New Group Records
--
-- Table: RAD_New_Collection_Groups
-- select * from rad_new_collection_groups order by customerkey, collectiongroupkey

--
-- Get stats for new records - EX3
--

truncate table rad_newattempts_stats_ex3;
insert into  rad_newattempts_stats_ex3 
select  ex2.collectiongroupkey, 
        count(ex2.collectionkey) NewAttempts, 
        max(ex2.collectionkey) MostRecentAttempt
from RAD_AttemptGroupAssoc_EX2 ex2
 group by ex2.collectiongroupkey
/
commit
/
 

truncate table RAD_New_Collection_Groups; --to be _EX4
insert into RAD_New_Collection_Groups
    select customerkey, 
    dense_rank() over (partition by customerkey order by collectionkey) CollectionGroup,
    RequestDate CollectionGroupStartDate,
    collectionkey CollectionGroupKey 
    from rad_newattempts_ex1
    where new_group_flag = 1;
 commit;
 
truncate table RAD_New_Collection_Groups_EX1;
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
 
 
truncate table RAD_New_Collection_Groups_EX3;
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
group by RAD_Collection_Groups.customerkey having max(RAD_Collection_Groups.collectiongroup) > 1
/
commit
/
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
    CASE when collected_flag = 1 then 1 else 0 END final_status_flag
    from RAD_New_Collection_Groups_EX1 ex1
        JOIN rad_newattempts_ex1 on rad_newattempts_ex1.collectionkey=ex1.collectiongroupkey
        LEFT OUTER JOIN  RAD_New_Collection_Groups_EX3 ex3 ON ex3.customerkey = ex1.customerkey;
commit;

--
-- Update finalized Actives to Final
-- Note that this is the GROUP update, not the DISPOSITION update
-- (Not sure yet if it needs to be moved to later, anyway, but want to note it)
--
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
 and nextgroupkey <> collectiongroupkey
 /
 commit
 /

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


 /* index(RAD_Collection_Groups)*/ newgroups

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
)
/
commit
/
-- Option 2 -- If the group has been collected, set final to 1
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
   ) 
    /
commit
/
--
-- Option 3: If the customer has cancelled
-- 
--
-- Which shouldn't be a problem, get the customer records and move along, but no..
-- TBD    
    
--select * from RAD_New_Collection_Groups_EX3

--select * from rad_collection_groups
-- Thinking that I may need to 

-- And Thinking that maybe this needs to be for the New attempts, not new groups


--
-- Not New Groups: Update Existing Group Records
-- Table: RAD_Active_Collection_Groups
-- 
truncate table RAD_New_Group_Stats;
 insert into RAD_New_Group_Stats
    select distinct  co.customerkey, 
    cg.collectiongroup, cg.collectiongroupstartdate, cg.collectiongroupkey, 
    cg.nextgroupkey, cg.nextgroupstartdate,
    ex3.NewAttempts,
    ex3.MostRecentAttempt
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


---ARGHHHHGLK
-- We're not figuring final attempt after the first setup.
-- So it's not getting duped because later attempts aren't going in.
--
--
-- Determine Dispositions and Update Groups
-- Table: RAD_Active_Collection_Groups
-- RAD_Collection_Group_Disp
create table   RAD_Collection_Group_Disp as
(select distinct  RAD_New_Group_Stats.*,    
--are these group attributes or attempt attributes?
--(I think they're being requested for attempts but actually they are group)
CASE 
    when DISP.RESPONSE = 'D' then null 
    else trunc( disp.requestdate)-collectiongroupstartdate END
        daystocollect,
 CASE   
    when DISP.RESPONSE = 'D' then null 
    else      attemptsingroup-1 END
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
        Group_Disposition_Detail        
from  RAD_New_Group_Stats   
JOIN ods_central.j2_ISPCOLLECTION disp 
    ON RAD_New_Group_Stats.customerkey = disp.customerkey 
        and collectionkey=finalattempt-->= collectiongroupkey and collectionkey < nextgroupkey        
--JOIN rad_newattempts_ex1 on rad_newattempts_ex1.collectionkey =     disp.collectionkey
--where exclusion_reason is not null
and ods_current_flag = 1
);
commit;

 
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
RAD_Collection_Group_Disp.group_disposition_detail
 
   
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

/
commit
/

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