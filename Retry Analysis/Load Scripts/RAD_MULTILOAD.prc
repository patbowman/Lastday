 create or replace procedure pbowman.RAD_Multiload(
    p_startdate   date DEFAULT NULL,
    p_enddate      date DEFAULT NULL
)
as
 --DECLARE 
 -- Here's the problem
 -- There's a unique index on one of my global temp tables
 -- And it makes this all run faster, but it also causes problems when for various reasons
 -- It's not actually unique
 -- SO - the idea here is to have something to run it a day at a time
 -- (which reduces the unique problem)
 -- And ignore the constraint errir if it does happen to come up.
 -- Right this moment it's doing a put line
 -- but if it's easier to do ABSOLUTELY NOTHING
 -- Then I'll go there.
 -- This is practically my first piece of real PL/SQL  
 -- Everything else has been just deconstructed sql, no control structures
 -- but this has a loop, dependent on a cursor, handled with an exception
 -- just like real code.
 CHECK_CONSTRAINT_VIOLATED EXCEPTION;  
 PRAGMA EXCEPTION_INIT(CHECK_CONSTRAINT_VIOLATED, -2290);
   v_reportperiod     VARCHAR (25);
   v_begin_date       DATE;
   v_end_date          DATE;
   v_ods_date          DATE;
 CURSOR rad_dates IS 
    select fulldate firstdate, fulldate+1 lastdate 
    from ods_central.dw_date where fulldate between p_startdate and p_enddate order by fulldate;
Begin
Open rad_dates;
Loop

    fetch rad_dates INTO v_begin_date, v_end_date;
    exit when rad_dates%NOTFOUND;
    begin
     ods_central.ods_retryanalysis(v_begin_date,v_end_date); commit; -- should clear out GT
      EXCEPTION
  WHEN CHECK_CONSTRAINT_VIOLATED THEN  -- catch the ORA-02290 exception
    null;--DBMS_OUTPUT.PUT_LINE('Failed due to check constraint violation'); 
    commit;-- should clear out GTs
   when others then  
    null;
    end;
end loop;
close rad_dates;
end;
    