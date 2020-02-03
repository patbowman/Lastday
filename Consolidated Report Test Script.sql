
with 
-- Get corporate IDs for which we run consolidated, to compare/count
corpids as

(select * from j2reports.rep_consol_corpIDs where status = 'A'),
-- Get corporate services that are in consolidate reports
servsum as
(
select billdate, corp_services.resellerid, count(*) ServiceCount from billops.corp_services
 RIGHT OUTER JOIN corpids ON corpids.resellerid = corp_services.resellerid
 where billdate = (select max(billdate) from billops.corp_services)
 group by billdate, corp_services.resellerid
), --select * from servsum
-- Get summary of data in the usage report
cons_datacount as
(select resellerid, count(*) RecordCount, 
sum( subscription) Subscription, sum(billable_amount) InboundAmount,
sum(BillablePages) InboundPages, sum(outpages) OutPages, sum(outamt) Outbound_Amount,
sum(CASE when resellerid is null then 1 else 0 END) NullResellerID,
sum(CASE when startdate is null then 1 else 0 END) Nullstartdate
 from j2reports.con_usage_report1 group by resellerid),

con_corp_service as
(select count(*) recordcount from J2REPORTS.CON_CORP_SERVICES),
con_corp_dids as
(select count(*) recordcount  from J2REPORTS.CON_EFAXNUMBERS1),
con_corp_inbound as
(select count(*) recordcount from J2REPORTS.CON_INBOUND_USAGE1),
con_corp_outbound as
(select count(*) recordcount from J2REPORTS.CON_OUTBOUND_USAGE1)
 
 
 --select * from cons_datacount JOIN servsum ON servsum.resellerid = cons_datacount.resellerid
 ,
 
 -- Start tests
 --
 --
-- Test 1 - Check for data
--
-- These first few are process steps, more for confirmation and troubleshooting
--
proc_test1 as
 
 (
 select
CASE when nvl(sum(recordcount),0) < 10 then 'Fail p2 - ' || nvl(sum(recordcount),0)  || 'Service Records available'

else 'Passed P1 - Service Data Exists'
end Test_Results
 FROM con_corp_service
 ),
proc_test2 as
 
 (
 select
CASE when nvl(sum(recordcount),0) < 10 then 'Fail p2 - ' || nvl(sum(recordcount),0)  || ' DID Records available'

else 'Passed P2 - DID Data Exists'
end Test_Results
 FROM con_corp_dids
 ),
proc_test3 as
 
 (
 select
CASE when nvl(sum(recordcount),0) < 10 then 'Fail p3 - ' || nvl(sum(recordcount),0)  || ' Inbound Records available'

else 'Passed P3 - Inbound Data Exists'
end Test_Results
 FROM con_corp_inbound
 ),
 proc_test4 as
 
 (
 select
CASE when nvl(sum(recordcount),0) < 10 then 'Fail p4 - ' || nvl(sum(recordcount),0)  || ' Outbound Records available'

else 'Passed P4 - Outbound Data Exists'
end Test_Results
 FROM con_corp_outbound
 ),
con_test1 as
 
 (
 select
CASE when nvl(sum(recordcount),0) < 10 then 'Fail 1 - ' || nvl(sum(recordcount),0)  || ' Records available'

else 'Passed 1 - Data Exists'
end Test_Results
 FROM cons_datacount
 ),
--
-- Test 2 - Check for data
--
con_test2 as
 
 (
 select
CASE when count(cons_datacount.resellerid) < count(corpids.resellerid) then 'Fail 2 - ' || count(cons_datacount.resellerid) || ' Processed ' || count(corpids.resellerid) || ' Exist'

else 'Passed 2 - Data Exists all resellers'
end Test_Results
 FROM cons_datacount RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 --
-- Test 3 - Inbound Amount Exists
--
con_test3 as
 
 (
 select
CASE when nvl(sum(cons_datacount.InboundAmount),0) <= 100 then 'Fail 3 - ' || sum(cons_datacount.InboundAmount) || ' Inbound Amount ' --|| count(corpids.resellerid) || ' Exist'

else 'Passed 3 - Data Exists Inbound Amount '
end Test_Results
 FROM cons_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
--
-- Test 4 - Subscription Amount Exists
--
con_test4 as
 
 (
 select
CASE when nvl(sum(cons_datacount.Subscription),0) <= 100 then 'Fail 4 - ' || sum(cons_datacount.Subscription) || ' Processed ' --|| count(corpids.resellerid) || ' Exist'

else 'Passed 4 - Data Exists Subscription Amount '
end Test_Results
 FROM cons_datacount RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 --
-- Test 5 - Inbound Pages Exists
--
con_test5 as
 
 (
 select
CASE when nvl(sum(cons_datacount.InboundPages),0) <= 100 then 'Fail 5 - ' || sum(cons_datacount.InboundPages) || ' Processed ' --|| count(corpids.resellerid) || ' Exist'

else 'Passed 5 - Data Exists Inbound Pages'
end Test_Results
 FROM cons_datacount RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 
  --
-- Test 6 - Outbound Pages Exists
--
con_test6 as
 
 (
 select
CASE when nvl(sum(OutPages),0) <= 100 then 'Fail 6 - ' || sum(OutPages) || ' Processed ' --|| count(corpids.resellerid) || ' Exist'

else 'Passed 6 - Data Exists Outbound Pages'
end Test_Results

 FROM cons_datacount RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),

--
-- Test 7 - No Null Start dates
--
contest7b as (select resellerid, sum(CASE when nullstartdate > 0 then 1 else 0 END) NullStartCount 
            from cons_datacount group by resellerid),

con_test7 as
 (
 select
CASE when nvl(sum(NullStartCount),0) >= 1 then 'Check Fail 7 - ' || sum(NullStartCount) || ' Resellers with null start dates '

else 'Passed 7 - No Null Start dates any resellers'
end Test_Results
 FROM contest7b --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 --where resellerid is not null -- hmmm. Without this, it does mean there are some null reseller IDs
 ),
 
--
-- Test 8 - No Null Resellers
--
con_test8 as
 
 (
 select
CASE when sum(nvl(RecordCount,1)) >0 then 'Check Fail 8 - ' || sum(nvl(RecordCount,1)) || ' Records with null resellerID ' --|| count(corpids.resellerid) || ' Exist'

else 'Passed 8 - Data Exists all resellers'
end Test_Results
 FROM cons_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 where resellerID is null
 ),
 con_test9 as
 
 (
 select
CASE when nvl(sum(Outbound_Amount),0) <= 100 then 'Fail 9 - ' || nvl(sum(Outbound_Amount),0) || ' Total Outbound Amount ' --|| count(corpids.resellerid) || ' Exist'

else 'Passed 9 - Data Exists Outbound Amount'
end Test_Results

 FROM cons_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 )
 select * from con_test1
 UNION ALL
  select * from proc_test1
 UNION ALL
   select * from proc_test2
 UNION ALL
   select * from proc_test3
 UNION ALL
   select * from proc_test4
 UNION ALL
 select * from con_test2 -- May not be valid test
  UNION ALL
 select * from con_test3
  UNION ALL
 select * from con_test4
  UNION ALL
 select * from con_test5
  UNION ALL
 select * from con_test6
  UNION ALL
 select * from con_test7 -- May not be valid test
  UNION ALL
 select * from con_test8 -- May not be valid test.
  UNION ALL
 select * from con_test9 ;
 --order by test_results desc
 -- select * from j2reports.con_usage_report1 where customerkey = 9047332 or serviceid = 10000651770
 --where customerkey is null
 -- resource_type is  null
 
 --select * from billops.corp_services
 --where serviceid = 10000651770
 --or customerkey = 9047332
 
 --give me a count of corpIDs without inbound pages
 
 
 with dupes as
 (
  select customerkey, count(distinct serviceid) dupecount, max(startdate) last_start, max(enddate) last_end 
  from j2reports.con_efaxnumbers1
 where resource_type = 'SEND_ONLY'
 group by customerkey  having count(*) >1
 ),
services_to_delete as ( select 
serviceid
 --s.*, dupes.*
  from billops.corp_services s JOIN dupes ON s.customerkey = dupes.customerkey
where startdate <> last_start
or (last_end<>enddate )), --or last_end is null))
 
 -- we're going to remove any that are not equal to the max start date
 -- and if end date is not null, not equal to the max end date

 consecutive_delete as
 (
 select 
'delete from j2reports.con_efaxnumbers1 where serviceid='''||s.serviceid||''';' as Delete_Script
 --,s.*, dupes.*
  from services_to_delete s 
)
--select 'delete from billops.corp_services where servicekey in (' ||
--(select servicekey || ',' from services_to_delete) || ')'
--union
--select * from dupes

select * from consecutive_delete;

/* November examples
select * from j2reports.con_efaxnumbers1 where customerkey in (41696140,
45336057)

servicekey in (74801499,
85491538)

41696140
45336057

10032260231
10037459130
*/