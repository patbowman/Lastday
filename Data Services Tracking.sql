--Step 1: Send a reply to the NOC letting them know that you're looking into the alert

--Step 2: Check the DI_Workflow_State table to see which workflow(s) are in a CRITICAL or WARNING state
-- This is on DWDB1_BIBO 
-- Pat, you have a separate Secret in Secret Server for this one.

--If the alert is for a workflow containing "WF_ODS_EM_" (Campaigner), "WF_ODS_CP"_ (Campaigner) or "WF_DM_" (Data Mart) 
-- please contact Vito or Vishnu 
42862299,
42862405

956017 -- Failed Saturday
956431 -- redid Saturday
956468 -- successful Saturday
957051 -- Failed Sunday
957556 -- successful Sunday
select workflowkey, workflow_name, overall_state, last_successful_run, most_recent_wf_status, start_time_of_current_run, next_expected_run, log_time 
from DWADMIN.DI_WORKFLOW_STATE 
where overall_state <> 'OK'
--workflowkey = 454
order by overall_state desc, most_recent_wf_status, workflow_name
958083 -- Failed Monday(fax accounts only)
958612 -- success monday (contacts only?)
959116 -- Failed Tuesday
960146 -- Failed Wednesday
970387 -- failed Saturday
971420 -- Failed Sunday
972452 -- failed monday
973491 -- failed Tuesday
974520 -- Failed Wednesday
975561 -- Failed Thursday
984140 -- failed back around to Friday, success was 984255
985203 -- Success Saturday
986279 -- success Sunday
986813 -- Failed Monday
987847 -- failed Tuesday
988109 -- contacts skipped Tuesday
988881 -- Failed Wednesday
989915 -- Failed Thursday
990946 -- Failed Friday
994037 -- Monday
996097 -- Wednesday
997139 -- Only Abandons failed (!) on Thursday
998170 -- Friday
999201 -- Satirday - going to be past a million by tomorrow.999713
999713,999201
1000235 -- sunday
1002280 -- Tuesday, fax abandon
1009510 -- Tuesday (4/11)
1019828
1020859  -- Saturday
1021890 -- Sunday
1022922 -- Monday
1033271 -- Thursday May 4
1036769 -- SUnday May 8--Step 3: Check the run history for the workflow in the DI_Workflow_Run table select * from dwadmin.di_workflow where workflow_name ='WF_ODS_EM_EmailAddressKeyLookup' ''where workflowkey = 243
select * from dwadmin.di_workflow_run --where jobrunkey = 958612
--where workflowrunkey = 20479815
--where workflowkey=466--<3flowKey(s) from step 2>449 -- 
--statuskey=6workflowrunkey, workflowkey
--and statuskey = 4
--and statuskey = 3
where statuskey in (6,7)
--and statuskey not in (1,3)
order by workflowrunkey desc
select * from dwadmin.di_job_run
where
select * from DWADMIN.DI_STATUS
     update dwadmin.di_workflow_run
set statuskey = 10
where workflowrunkey in 
(46965614,
46965610,
46965601,
46965598,
46965589,
46965585,
46965576,
46965573,
46965564,
46965562,
46965551
)

(22266761,
22266299,
22265857,
22265532,
22265531)
select  w.workflowkey, workflow_name, warning_hour, critical_hour, j.jobkey, job_name, ignore_critical_status_until--, w.*  
,j.*
from dwadmin.di_job j
JOIN dwadmin.di_job_workflow jw ON jw.jobkey = j.jobkey
JOIN dwadmin.di_workflow w ON W.WORKFLOWKEY=JW.WORKFLOWKEY
--JOIN dwadmin.
--where j.jobkey = 21
--where w.workflowkey = 66
where w.workflowkey in  (select workflowkey
from DWADMIN.DI_WORKFLOW_STATE 
where overall_state <> 'OK'
)
contacts success 
986813,
985776,
984746,986279,985203

214
331
226
231
234
322

select * from dwadmin.di_workflow --where workflow_name like '%UV_Service%'
where workflow_name like '%Send%' 
--218 WF_ODS_DW_SendFaxCompletionHourly
where workflowkey = 439

-- 449 warning hour is 12, critical is 13
-- It usually starts about 11, and takes about an hour and a half
-- So it's almost always going to throw a warning.
--If we 
--Step 4: If the workflow has DI_Workflow_Run.StatusKey 4 (FATAL) or 3 (ERROR) log into Data Services Management Console to check the error log
-- http://bietl1.j2noc.com:8080/DataServices/launch/logon.do
-- Pat, your Data Services password is the one you used for this.
-- Looks like Administratror then Batch and Primary_Repo and Job Error Log.
-- If the error is something that may only be a temporary issue, such as losing connection to a database, you should only need to pause the alert, re-set any FATAL statuses if necessary and allow the process to run at its next scheduled time
-- If the error is more than something like a simple database connection issue please contact Vito or Vishnu
-- One particular issue to keep in mind is on Friday nights the data warehouse weekly refresh sometimes interrupts workflows that are running. When this happens, the workflow will get stuck with DI_Workflow_Run.StatusKey 6 (STARTED) and will need to have its StatusKey updated to 3 (ERROR) before it will be able to run again.
-- Workflows that have DI_Workflow_Run.StatusKey 4 (FATAL) will need the StatusKey updated to 9 (RECOVERED) before they can run again.

--Step 5: If necessary, update the DI_Workflow_Run.StatusKey
     update dwadmin.di_workflow_run
set statuskey = 3 --<new statuskey>
--where workflowkey = 237 and statuskey = 4
--where workflowkey = 399 and statuskey = 7
--where workflowkey = 226 and statuskey = 6
--where (workflowkey = 243 and workflowrunkey = 21569417) -- Contacts
--where workflowkey = 224 and workflowrunkey in (20841041)
where (workflowkey = 378 and workflowrunkey=23245770) 

(workflowkey = 226 and workflowrunkey=22109506) 
or  (workflowkey = 198 and workflowrunkey=22109507)
or  (workflowkey = 214 and workflowrunkey=22109500)
or  (workflowkey = 411 and workflowrunkey=22109343)
or  (workflowkey = 213 and workflowrunkey=22109501)

or  (workflowkey = 226 and workflowrunkey=21025801)
or  (workflowkey = 214 and workflowrunkey=21025824)
where (workflowkey = 198 and workflowrunkey=21025831)
(workflowkey = 331 and workflowrunkey=21025825) -- Batch History
or  (workflowkey = 214 and workflowrunkey=21025824)
or  (workflowkey = 226 and workflowrunkey=21025801)
or  (workflowkey = 214 and workflowrunkey=21025824)
18264573)-- =18261768
where workflowkey = 259 and workflowrunkey=18219267
and statuskey=4
and workflowkey = 416
select * from dwadmin.di_workflow where workflowkey=208
where workflow_name like '%Contact%'
where workflowkey=218 --<WorkflowKey(s) from step 2>449 -- 
--statuskey=6workflowrunkey, workflowkey
959116
workflowrunkey  in
(18195365,
18194906,
18194470,18193959
)
18175889,
18175610)

(18163590)
18137878)
18137316,
18137177,
18137033,
18136874,
18136715,
18136544,
18136397)

18137661,
18137660,
18137659,
18137658,
18137657,
18137654,
18137653,
18137650,
18137649,
18137647,
18137646,
18137645,
18137643,
18137642,
18137641,
18137640,
18137639,
18137638,
18137637,
18137636,
18137635,
18137631,
18135995,
18135991,
18135987,
18135963,
18135958,
18135192) AND STATUSKEY = 6
= <WorkflowRunKey(s) from step 4>
select * from dwadmin.di_job_workflow where workflowkey in (260)
--Step 6: If necessary, pause the alert on the workflow by updating the DI_Job_Workflow.Ignore_Critical_Status_Until column
update dwadmin.di_job_workflow
set ignore_critical_status_until = to_date('2020/01/09:07:00:00PM', 'yyyy/mm/dd:hh:mi:sspm') --where  jobkey=18 workflowkey=228
where workflowkey in (466)(109, 401,403)
(170,
169,
165,
173,
172)

 (246,244,243,242,259,451)
98,5,66)
 (403,
244,
451,
210,
214,
101,
401,
259,
450,
449,
260,
404,
459,
200,
118,
454,
245,
213)
--where workflowkey in (226,227,228,229,198,199)
--where workflowkey in(246,228,259)
--where workflowkey in (117),224)
--where workflowkey in( 460, 172, 448, 173,439, 440)20
--where workflowkey in(172,173,439,440)20
--where -- workflowkey in(173,440)-
where workflowkey in (449,
451,
450,
244
259)
where workflowkey in (260,
247,
226,
227,
229,
228,
198,
199,
201,
200,
117)
where workflowkey in (429,225)
workflowkey in(401,
226,
227,
229,
228,
117)
where workflowkey in (165,172)
where workflowkey in (260,245)
where workflowkey in (--246,
243,454,
260,
259)

 (--449,
246,
259,
454, 243)
where workflowkey in (260,454)
449,
245)
 (160)
where workflowkey in (244,
451,
454,
450)
where workflowkey in (259,454,243)
where workflowkey in (401)
where workflowkey in (218, 226, 117)
where workflowkey in (226,
227,
229,
228,
259,
198,
199,
117)
,  
244,
451,
450)
,
244,
450,
451)
160,
450,
451)
198,
331,
214,
227,
213,
392,
229,
228)

)(259)

select * from dwadmin.di_job_workflow where jobkey = 6 workflowkey = 228
259
259,
278,
281,
7,
8,
9,
10,
11,
12,
13,
18,
19,
21,
25,
26,
78,
180,
181,
378,
90,
95,
96,
97,
98,
144,
147,
149,
194,
195,
196,
214,
1,
2,
3,
4,
5
)

WORKFLOWKEY in (
450,
451,
454,
245,
259,
260,
244)

<WorkflowKey(s) from step 2>

--So for a particular workflow, what I''ll often want to know is
-- Workflowkey
--Workflowname
--Job Key
--Job Name
-- Ignore Critical Status
-- Warning
-- Critical hour


select * from dwadmin.di_job where jobkey = 6
select * from dwadmin.di_job_workflow where jobkey=21
where workflowkey in (218)
259,
278,
281,
7,
8,
9,
10,
11,
12,
13,
18,
19,
21,
25,
26,
78,
180,
181,
378,
90,
95,
96,
97,
98,
144,
147,
149,
194,
195,
196,
214,
1,
2,
3,
4,
5
)

172,173,439,440,460)

select workflowkey --, workflow_name, overall_state, last_successful_run, most_recent_wf_status, start_time_of_current_run, next_expected_run, log_time 
from DWADMIN.DI_WORKFLOW_STATE
where overall_state =  'CRITICAL'
order by overall_state, most_recent_wf_status, workflow_name

PBOWMAN@DWDB1_BIBO.WORLD    Finished    2:34:57 PM    2:34:57 PM    21 msecs    Select    5    select workflowkey, workflow_name, overall_state, last_successful_run, most_recent_wf_status, start_time_of_current_run, next_expected_run, log_time from DWADMIN.DI_WORKFLOW_STATE
where overall_state <> 'OK'
order by overall_state, most_recent_wf_status,PBOWMAN@DWDB1_BIBO.WORLD    Finished    2:36:31 PM    2:36:31 PM    18 msecs    Update    2    update dwadmin.di_job_workflow
set ignore_critical_status_until = to_date('2016/09/09:04:00:00PM', 'yyyy/mm/dd:hh:mi:sspm')
--where workflowkey in( 460, 172, 448, 173,439, 440)
--where workflowkey in(172,173,439,440)
--where -- workflowkey in(173,440)
whPBOWMAN@DWDB1_BIBO.WORLD    Finished    2:36:31 PM    2:36:31 PM    18 msecs    Update    2    
select * from dwadmin.di_job_workflow where workflowkey in (160)
update dwadmin.di_job_workflow
set ignore_critical_status_until = to_date('2019/04/28:05:30:00PM', 'yyyy/mm/dd:hh:mi:sspm')

where workflowkey in (208,
99,
101,
401,
9,
403)
--where workflowkey in (246,247)
--where workflowkey in( 460, 172, 448, 173,439, 440)
--where workflowkey in(172,173,439,440)
--where -- workflowkey in(173,440)
--where workflowkey in (160)
where workflowkey in (101,
260,
99,
244,
454,
450,
259,
451,
46)
where workflowkey in (8)
where workflowkey in (399,
243,
109,
66,
108,
452,
246,
247,
176,
177,
103,
102,
214,
213,
67)
where workflowkey in (244,
451,
450)--246,
--247,
244,
--160,
450,
451)

select * from  dwadmin.di_job_workflow where workflowkey = 321