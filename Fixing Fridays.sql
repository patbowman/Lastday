--
-- Fixing Fridays
--
--
-- This checks DWADMIN for workflows that are in STARTED or INITIALIZING status and are not in an OK status.  
-- I use it to restart workflows that might be stuck because of the Stage refresh on Fridays.
-- (Can be used if there are other reasons that a bunch of workflows are stuck.) 
--
update dwadmin.di_workflow_run
set statuskey = 10            -- 10 is Abnormal, which effectively kills any run attempts for the identified workflow(s)
--select * from dwadmin.di_workflow_run 
where workflowkey in 
(
select workflowkey
-- workflow_name, overall_state, last_successful_run, most_recent_wf_status, start_time_of_current_run, next_expected_run, log_time 
from DWADMIN.DI_WORKFLOW_STATE 
where overall_state <> 'OK' -- if status is still okay, it's not having trouble yet.
)

and statuskey not in (1,2, 3,4,9, 5,10,8) -- Yeah, I coulda used IN (6,7) but never felt like changing something that worked
order by workflowrunkey desc

