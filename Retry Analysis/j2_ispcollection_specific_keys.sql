 with big17 as
 ( select distinct customerkey, collectionkey, response, responsecode, processorgroup, requestdate, 
     typecode Card_Type, 
     currencycode, amount,
     country billingcountry,
     marketingregion,
     RetryCount,
     CASE 
     when  nvl(preauthflag,0) in (-1,1) then 'Preauth' 
     when  processorgroup='P' then 'Preauth' 
     when response='C' then 'Cancelled' 
     else null END 
        Exclusion_reason,
     CASE 
        when retrycount <> 0  then 0 -- non zero retry is not a new group
        when response in ('C') then 0 -- Cancelled records are not a new group
        when amount < 0 then 1 -- Refunds act like new groups in any case
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
    where   requestdate >= '01-JAN-2015' 
    and requestdate < '01-JAN-2016'
    -- requestdate between '01-JAN-2016' and '02-JAN-2016'
     --and nvl(preauthflag,0) not in (-1, 1) -- no preauths here
      and co.ods_current_flag = 1
            and response is not null 
       order by requestdate
      )
      select collectionkey from big17 group by collectionkey having count(*)>1
      
      select * from ods_central.j2_ispcollection
      where collectionkey in (128621446,
128621909,
128621918,
128591896,
128609403,
128609559,
128610165,
128611564,
128612622,
128612832,
128613923,
128621829,
128621695,
128621617,
128621440,
128621498,
128599050,
128595032,
128462949,
128610431,
128611871,
128611928,
128612186,
128610993,
128612875,
128465367,
128613738,
128613921,
128611545,
128613928,
128621792,
128588679,
128621418,
128621558,
128621898,
128608141,
128621549,
128621701,
128593355,
128608879,
128609560,
128609592,
128612568,
128612617,
128613237,
128613770,
128621580,
128621929,
128621702,
128621516,
128609038,
128589823,
128621571,
128621649,
128621651,
128621662,
128621880,
128592722,
128464836,
128611411,
128621577,
128621655,
128601323,
128621425,
128463265,
128608600,
128611652,
128464926,
128621604,
128613316,
128621631,
128600265,
128611961,
128429102,
)
and ods_current_flag = 1
order by collectionkey