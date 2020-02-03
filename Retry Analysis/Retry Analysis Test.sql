--
-- Extracts
--

with RA_sample as
(
      select
   RA_custsubset.customerkey,
   RA_custsubset.collectionkey,
    MarketingRegion,
    BillingCountry,
    currencycode,
   requestdate,
  -- collectiondate,it's in here but doesn't DO anything.
   processorgroup,
   daystocollect,
   declinesuntilsuccessful,
   

            dense_rank() over (partition by collectiongroupkey order by collectionkey) AttemptNumberinGroup,
       collectionkey DispCollKey, RA_cgroupsdisp.collectiongroupkey , RA_cgroupsdisp.nextgroupkey --,
       --RA_cgroupsdisp.* --, custsubset.*
   
       from RA_cgroupsdisp  -- RIGHT OUTER JOIN custsubset ON    --   and

JOIN RA_custsubset ON RA_cgroupsdisp.customerkey = RA_custsubset.customerkey 
AND
(
 (RA_custsubset.collectionkey >= RA_cgroupsdisp.collectiongroupkey and RA_custsubset.collectionkey < RA_cgroupsdisp.nextgroupkey )  
 OR
( RA_cgroupsdisp.collectiongroupkey = RA_cgroupsdisp.nextgroupkey and  RA_custsubset.collectionkey >RA_cgroupsdisp.nextgroupkey)
)

where requestdate between '01-JUN-2019' and '02-JUN-2019'
--or RA_custsubset.customerkey = 44860059
--and group_disposition_detail <> 'Paid on first attempt'
--and group_disposition = 'Declining'
order by RA_custsubset.customerkey, requestdate
),
validsource as (
select * from ods.j2_ispcollection co
where requestdate between '01-JUN-2019' and '02-JUN-2019'
and ods_current_flag = 1

and response not in ('C')       
and nvl(preauthflag,0) not in (-1, 1)   -- don't count pre-auths for this  
),
--
-- Tests
-- 
individual_tests as
(select
CASE when count(ra_sample.collectionkey) <> count(validsource.collectionkey) then 'Fail 1 - Indivdual collectionkey count does not match - target ' || count(ra_sample.collectionkey) ||' source '|| count(validsource.collectionkey)

else 'Pass - individual count'
end Test_Results
FROM RA_Sample

RIGHT OUTER JOIN validsource ON validsource.collectionkey = RA_Sample.collectionkey -- same records in both
),
aggregate_tests as
(
select
CASE when count(ra_sample.collectionkey) <> count(validsource.collectionkey) then 'Fail 2 - Aggregate collection key count does not match - target ' || count(ra_sample.collectionkey) ||' source '|| count(validsource.collectionkey) 

else 'Pass - Aggregate Count'
end Test_Results
/*
when count(distinct yearnumber) < 1 then 'Fail 1  - ' || count(distinct yearnumber) -- no years, which should mean no data

when count(distinct yearnumber) < 1 then 'Fail 1  - ' || count(distinct yearnumber) -- no years, which should mean no data
        when count(distinct monthnumber) < 2 then 'Fail 2  - '|| count(distinct monthnumber)-- not full count of months
   --     when count(distinct monthnumber) = count(monthnumber) then 'Fail 2.5  - '|| count(monthnumber) -- duplicate months Doesn't work with multiple years
        when min(yearnumber) >2013 then 'Fail 3  - ' || min(yearnumber) -- doesn't include 2015
        when min(yearnumber) >2015 then 'Fail 4  - ' || min(yearnumber) -- doesn't include 2015
        when max(yearnumber) > 2016 then 'Fail 5  - '|| max(yearnumber) -- has years after 2016
        when min(monthnumber)>10 then 'Fail 6 - ' || min(monthnumber) -- Missing some month at the start
        when max(monthnumber) <11 then 'Fail 7  -' || max(monthnumber) -- Missing some month at the end
        when max(bill_amount)<=0 then 'Fail 8 -' || max(bill_amount) -- no bill amounts or only zero/negative
        when min(bill_amount)<=0 then 'Fail 9 -' || min(bill_amount) -- no monthly bill amounts should be zero or negative
        when max(bill_amount) is null then 'Fail 10 - Null result' -- May want a better way to show a fail if it's empty. 
        else 'Pass' END Test_Results,
        */
FROM RA_Sample

right OUTER JOIN validsource ON validsource.collectionkey = RA_Sample.collectionkey -- same records in both
)
--
-- Output
--

select * from aggregate_tests
UNION ALL
select * from individual_tests 



 
   