select max(requestdate) from ods_central.rp_retryanalysis
select trunc(requestdate), count(*), count(distinct collectionkey) from ods_central.rp_retryanalysis
group by trunc(requestdate) 
order by trunc(requestdate) desc;

select trunc(collectiongroupstartdate), count(*) from ods_central.rad_collection_groups
group by trunc(collectiongroupstartdate) 
order by trunc(collectiongroupstartdate) desc;


select r.*, case when final_status_flag =0 then 0 when Group_Disposition <> 'Paid' then 0 else Amount END Collected_Amount
  from ods_central.rp_retryanalysis r where requestdate between '09-DEC-2019' and '15-DEC-2019'
and group_disposition_detail like 'C%'