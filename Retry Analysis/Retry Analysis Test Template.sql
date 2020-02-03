--
-- Yet Another Testing Template
-- 
-- This is set up in SQL as a query that outputs straight text list 
-- that says what passed and what failed.
---
-- Three Main Sections
--- Extracts - separate out extracts as individual queries
--- Tests - yields individual test results using extracts
--- Select with Union - Show the results
--
-- Goal is to have a suite of tests in one place that can be easily re-run
-- And used whenever there is a change.
-- And updated with appropriate tests for that change.


with 

---
--- Extracts
---
-- Get the data you'll need. The with clause is a way to consolidate the code
-- And when it's reusable, it'll be there.

RP_retry as -- Customer Change table
(
select * from ods_central.rp_retryanalysis where requestdate
between '01-DEC-2019' and '05-DEC-2019'
--LEFT OUTER JOIN select *  from ods.j2_creditcard
),
     
CO_source as -- Customer Dimension records, to be tested
(
select * from ods.j2_ispcollection where requestdate
between '01-DEC-2019' and '05-DEC-2019'
and ods_current_flag = 1
),
 
QA_CreditCards as -- List of known QA credit cards
(
select * from ods.j2_creditcard where
    (ACCOUNTNUMBER in ('XXXXXXXXXXXX3055','XXXXXXXXXXXX0680','XXXXXXXXXXXX0585','413373XXXXXX3055','471563XXXXXX0680','464018XXXXXX0585')) 
    and ods_current_flag = 1 
),
RP_datacount as
(select count(*)RecordCount,count(distinct collectionkey) Distinct_Collections, count(distinct customerkey) Distinct_Customers
     from RP_Retry),
     
CO_datacount as
(select count(*)Source_Changes,count(distinct collectionkey) Distinct_Collections, count(distinct customerkey) Distinct_Customers
     from CO_source),
CO_Dupes as
(
select collectionkey, count(*) Multiples_Count,count(*)-1 Dupes_Count
from CO_source
group by collectionkey having count(*) > 1 
),
/*
--select * from servsum
-- Get summary of data in the usage report
CD_datacount as
(select count(*) RecordCount, 
sum(CASE when qa_account is null then 0 else 1 END) QA_Account_Update_Count,
sum(CASE when GDPR_OPT_IN_FLAG is null then 0 else 1 END) GDPR_Optin_Update_Count,
--sum(CASE when qa_account is null then 0 else 1 END) QA_Account_Update_Count,
--sum( subscription) Subscription, sum(billable_amount) InboundAmount,
--sum(BillablePages) InboundPages, sum(outpages) OutPages, sum(outamt) Outbound_Amount,
sum(CASE when reseller_id is null then 1 else 0 END) NullResellerID,
sum(CASE when start_date is null then 1 else 0 END) Nullstartdate --,
--sum(CASE when website_tracker is null then 1 else 0 END) NullWebsiteTracker
 from CD_TESTS),
 --select * from cons_datacount JOIN servsum ON servsum.resellerid = cons_datacount.resellerid
*/ 
 --
 -- Start tests
 --
 --
 comp_datacounts as
 (
 select 'RP' Item, recordcount RecordCount from RP_datacount
 union
 select 'Collection', Distinct_Customers from CO_datacount
 ),
 /*
 QA_Check as
 (select count(*) Bad_QA from CD_Tests
 --LEFT OUTER JOIN  QA_CREDITCARDS on QA_CREDITCARDS.CUSTOMERKEY=cd_tests.customer_key
where QA_Account = 'Y'
 and customer_key not in (select customerkey from QA_creditcards)
 ),
 */
--- TESTS
---
--- Tests are also set up as with clauses
--- Each outputs a single text string with the results
--- The extracts allow us to keep this part simple and consistent
--- Allowing them to be easily output and read
---

 
--
-- Test 1 - Check for data
--
RA_test1 as
 
 (
 select
CASE when sum(recordcount) < 10 then 'Fail 1 - ' || sum(recordcount) || ' Records'

else 'Passed 1 - Data Exists'
end Test_Results
 FROM RP_Datacount
 ) /*,
--
-- Test 2 - No duplicated collection records
--

RA_test2 as
 
 (
 select
CASE when sum(QA_Account_Update_Count) < sum(recordcount) then 'Fail 2 - ' || sum(QA_Account_Update_Count) || ' Processed ' || sum(recordcount)|| ' Exist'

else 'Passed 2 - Data Processed all QA Account'
end Test_Results
 FROM CD_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
--
-- Test 3 - Check for GDPR update
--

RA_test3 as
 
 (
 select
CASE when sum(GDPR_Optin_Update_Count) < sum(recordcount) then 'Fail 3 - ' || sum(QA_Account_Update_Count) || ' Processed ' || sum(recordcount)|| ' Exist'

else 'Passed 3 - Data Processed all GDPR opt in flags'
end Test_Results
 FROM CD_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
--
-- Test 4 - Confirm no null start dates
-- Unlikely to be wrong, but it's an easy check
--
 RA_test4 as
 
 (
 select
CASE when sum(Nullstartdate) >0  then 'Fail 4 - ' || sum(Nullstartdate) || ' Processed ' || sum(recordcount)|| ' Exist'

else 'Passed 4 - Data Processed no null start dates'
end Test_Results
 FROM CD_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 
 --
 -- Test 5 - QA account identified only if QA credit card used
 --  (Doesn't try to check anything else, only that accounts id'ed as QA use the card)
 -- 
RA_test5 as
 
 (
 select
CASE when sum(Bad_QA) >0  then 'Fail 5 - QA Account without Credit Card ' || sum(Bad_QA) --|| --' with dupes ' || sum(Multiples_Count)|| ' in total'

else 'Passed 5 - QA Account only with Credit Card'
end Test_Results
 FROM QA_Check --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 
 --
-- Test 6 - Check for duped customer keys
-- Unlikely to be wrong, but it's an easy check
--

 CD_test6 as
 
 (
 select
CASE when count(customer_key) >0  then 'Fail 6 - ' || count(customer_key) || ' with dupes ' || sum(Multiples_Count)|| ' in total'

else 'Passed 6 - No Duped Customer Keys'
end Test_Results
 FROM CD_Dupes --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 
--
-- Test 7 - Check for null website_tracker fields
-- Part of the addition of that field (UBI-1849)
-- 
-- This template won't run with clauses that aren't used, 
-- But it will check for correct syntax and available objects
-- So have to comment them out if they require unavailable fields.
--
 
 /*
  CD_test7 as
 
 (
 select
CASE when sum(NullWebsiteTracker) >0  then 'Fail 7 - ' || sum(NullWebsiteTracker) || ' Processed ' || sum(recordcount)|| ' Exist'

else 'Passed 7 - Data Processed no null start dates'
end Test_Results
 FROM CD_datacount --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 ),
 */
--
-- Test 101 -- compare input and output result sets
-- 
-- With ODS these won't always be exactly the same, 
-- So checking for a variance. 
-- (Dividing by 100 means it will succeed if within 1%; adjust if desired.)
-- The variance is appended to the result output regardless.
-- The Customer extract takes a while for this one - 10-20 minutes per day of data.
--
/*
CD_test101 as
 (
 select
--CASE when sum( CASE when item='Cust' then RecordCount else 0 END)- sum( CASE when item='CD' then RecordCount else 0 END)) > sum( CASE when item=='CD' then RecordCount else 0 END)/10 then 'Fail 5 - ' || sum( Distinct_Customers)|| ' Customers with null start dates '||sum(RecordCount)
CASE when max(  RecordCount )- min(  RecordCount) > max( RecordCount )/100  then 'Fail 101 - ' || max(recordcount)|| ' max '||min(RecordCount)|| ' min '
else 'Passed 101 - Input changes roughly equal output'
end || ': ' ||round((1-max(recordcount)/min(recordcount))*100,2)||'% diff'
Test_Results
 FROM comp_datacounts --RIGHT OUTER JOIN corpids ON corpids.resellerid = cons_datacount.resellerid
 --where resellerid is not null -- hmmm. Without this, it does mean there are some null reseller IDs
 )
*/
---
--- OUTPUT
--- 
--- Easy enough now: Run the tests, output the results
--- Union All since we don't need an actual Union (which requires a sort)
--- And Union can be confusing when adding new tests.
---
--- And now anyone who can run this script can confirm results:
--- If they all say Passed, it passed
--- If any say Failed, it did not. 
 
 select * from RA_test1
 UNION ALL
 select * from CD_test2 -- May not be valid test
  UNION ALL
 select * from CD_test3
    UNION ALL
 select * from CD_test4
     UNION ALL
 select * from CD_test5
    UNION ALL
 select * from CD_test6
 
--
-- Tests past here are slower and may be worth skipping in some situations
--
   UNION ALL
-- Takes about 10-20 minutes for one day of data
select * from CD_test101
 



-- This should run as-is if there is access to both ODS and J2DW tables
-- 
-- To add new tests
-- 1. Add the following (uncommented) to the end of the Output section
--   UNION ALL
-- select * from CD_test6
--
-- 2. Make a copy of one of the Test clauses,change the name and create the test
-- 3. If needed, set up an Extract clause to support the test clause.
----  For some tests, an addition to an existing Extract may be sufficient
---- (the Website Tracker test extract was added to CD_datacount, for example)  
--
--