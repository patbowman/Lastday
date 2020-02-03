begin ods_central.ods_retryanalysis('01-JAN-2015','02-JAN-2015'); end;
begin ods_central.ods_retryanalysis_B('02-JAN-2016','03-JAN-2016'); end;
begin ods_central.ods_retryanalysis('03-JAN-2016','04-JAN-2016'); end;
begin ods_central.ods_retryanalysis_B('04-JAN-2016','05-JAN-2016'); end;
begin ods_central.ods_retryanalysis_D('05-JAN-2016','06-JAN-2016'); end;


/*
truncate table  ods_central.rad_final
select * from ods_central.rad_newattempts_ex1
where collected_flag <> 1 and new_group_flag <> 1 and exclusion_reason is  null
and response is not null
select * from ods_central.rad_final where group_disposition <> 'Paid'
select * from 
*/