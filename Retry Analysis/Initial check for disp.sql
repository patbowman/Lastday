WITH gt_rad_newattempts_ex1 as
(
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
    where   requestdate >= '01-JAN-2016' 
    and requestdate < '02-JAN-2016' 
     --between '01-JUL-2019' and '02-JUL-2019'
     --and nvl(preauthflag,0) not in (-1, 1) -- no preauths here
      and co.ods_current_flag = 1 order by requestdate)
--insert into  GT_RAD_dispattempts_ex2
select collectionkey, customerkey, response, processorgroup, retrycount, requestdate, amount, ods_current_flag
from ods.j2_ispcollection where 
--ods_current_flag = 1 
--and 
customerkey in (select customerkey from GT_RAD_newattempts_ex1)
and requestdate between '01-JAN-2015' and '01-JAN-2016' -- right now, use a year before start date, though that's overkill.
and ods_current_flag =1;
