with customers as 
(select customerkey from ods.j2_ispcustomerchange
where ods_current_flag = 1
and startdate between '28-DEC-2018' and '31-DEC-2018'--),
and customerkey in (49677863)),
totals as 
(
select billingcustomerkey customerkey, sum(CASE when type = 'P' then DEBITAMOUNT-CREDITAMOUNT else 0 END) SettledNet, sum(CASE when type = 'S' then CREDITAMOUNT-DEBITAMOUNT else 0 END) ChargeNet
from ods.j2_ispjournal
where billingcustomerkey in (select customerkey from customers)
and ods_current_flag = 1
group by billingcustomerkey)
select customers.customerkey, totals.*, CASE when settlednet > 1 then 'Paid' else 'Not Paid' END Paid_Status from totals
RIGHT OUTER JOIN customers ON customers.customerkey = totals.customerkey
--49664211
--49664176
--49663973
--49663780
--50934845

select sum(CASE when type = 'P' then DEBITAMOUNT-CREDITAMOUNT else 0 END)  from ODS.j2_ispjournal where 49677863 = billingcustomerkey and ods_current_flag=1
