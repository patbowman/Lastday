with GetAccountsForTheseDays as
(
select * from ods_central.j2_ispcollection
where requestdate between '01-JAN-2015' and '27-JUN-2016'
and ods_effective_from between '01-JAN-2015' and '27-JUN-2016'
--and accountkey = 45949026
order by collectionkey, ods_effective_from,ods_effective_to
),AffectedRecords as
(select collectionkey, count(*), min(ods_effective_from) earlier, max(ods_effective_from) later 
from GetAccountsForTheseDays
where ods_current_flag =1
group by collectionkey having count(*) = 2 -- this process assumes two records and won't work with more

)
select GetAccountsForTheseDays.collectionkey as ChangeThisRecord, 
ods_effective_to as OverwriteThis, later as WithRealToTimestamp,ods_effective_from as LeaveThisFromDate,
ods_current_flag as ChangeToZero,
 AffectedRecords.*, GetAccountsForTheseDays.*
  from GetAccountsForTheseDays 
JOIN AffectedRecords  on GetAccountsForTheseDays.collectionkey=AffectedRecords.collectionkey
where ods_current_flag=1 and ods_effective_from=earlier