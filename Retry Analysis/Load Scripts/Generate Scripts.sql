select'begin ods_central.ods_retryanalysis('||to_char(startdate.fulldate,'''dd-MON-yyyy''')||','''||to_char(enddate.fulldate+1,'dd-MON-yyyy')||'''); end;' scriptease,
 startdate.fulldate-1, enddate.fulldate+1 from ods_central.dw_date startdate
JOIN ods_central.dw_date enddate ON startdate.monthnumber=enddate.monthnumber and STARTDATE.YEARNUMBER=enddate.yearnumber and STARTDATE.MONTHNUMBER=enddate.monthnumber
where enddate.islastdayofmonth = 'Yes' and startdate.monthdaynumber =1
and startdate.fulldate between '01-DEC-2013' and sysdate
order by startdate.fulldate;

-- Generate a single month by day

select'begin ods_central.ods_retryanalysis('||to_char(startdate.fulldate,'''dd-MON-yyyy''')||','''||to_char(startdate.fulldate+1,'dd-MON-yyyy')||'''); end;' scriptease,
 startdate.fulldate, startdate.fulldate+1 from ods_central.dw_date startdate
--JOIN ods_central.dw_date enddate ON startdate.monthnumber=enddate.monthnumber and STARTDATE.YEARNUMBER=enddate.yearnumber and STARTDATE.MONTHNUMBER=enddate.monthnumber
where --enddate.islastdayofmonth = 'Yes' and startdate.monthdaynumber =1


 startdate.fulldate between '01-JUL-2014' and '01-AUG-2014'
order by startdate.fulldate; --, enddate.fulldate