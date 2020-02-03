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
      and co.ods_current_flag = 1 order by requestdate),
previousgroup as
 ( select * --ex1.customerkey, nvl(max(a2.collectionGROUPkey),0) Active_CollectionKey 
    from  RAD_Collection_Groups a2 
--    RIGHT OUTER JOIN ods_central.GT_RAD_newattempts_ex1 ex1 ON a2.customerkey =ex1.customerkey  and final_status_flag =0 
   -- group by ex1.customerkey
    )
    select * from previousgroup
   -- select count(*) from RAD_Collection_Groupscommit