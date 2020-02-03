select'DECLARE CHECK_CONSTRAINT_VIOLATED EXCEPTION;  PRAGMA EXCEPTION_INIT(CHECK_CONSTRAINT_VIOLATED, -2290);
begin ods_central.ods_retryanalysis('||to_char(startdate.fulldate,'''dd-MON-yyyy''')||','''||to_char(startdate.fulldate+1,'dd-MON-yyyy')||''');'||
'EXCEPTION
  WHEN CHECK_CONSTRAINT_VIOLATED THEN  -- catch the ORA-02290 exception
    DBMS_OUTPUT.PUT_LINE(''Failed due to check constraint violation'');' ||
 -- WHEN OTHERS THEN                     -- catch all other exceptions
 --   DBMS_OUTPUT.PUT_LINE(''''Something else went wrong - '''' || SQLCODE ||
--                         ' : ' || SQLERRM);
                        '  end;' scriptease,
 startdate.fulldate, startdate.fulldate+1 from ods_central.dw_date startdate
--JOIN ods_central.dw_date enddate ON startdate.monthnumber=enddate.monthnumber and STARTDATE.YEARNUMBER=enddate.yearnumber and STARTDATE.MONTHNUMBER=enddate.monthnumber
where --enddate.islastdayofmonth = 'Yes' and startdate.monthdaynumber =1


 startdate.fulldate between '13-JAN-2020' and '15-JAN-2020'
order by startdate.fulldate;