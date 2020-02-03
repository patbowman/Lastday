select CASE when nvl(preauthflag,0) = 1 then 'Preauths' 
when response = 'C' then 'Cancelled attempts'
else 'Actual attempts to count' end AttemptType,
count(*) AttemptCount
--*
 from ods.j2_ispcollection --where collectionkey = 193930216 declined payment....
where customerkey =40763324
and ods_current_flag = 1
group by CASE when nvl(preauthflag,0) = 1 then 'Preauths' 
when response = 'C' then 'Cancelled attempts'
else 'Actual attempts to count' end 
--order by requestdate desc

-- 13 days to collect 100870644...nope, it was an immediate collection, There was a pre-auth 13 days later, because of a credit card change but it wasn't a REQUIRED one.