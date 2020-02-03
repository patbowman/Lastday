select rad_new_collection_groups_ex1.*,
  max(collectiongroupkey) OVER (partition by customerkey order by collectiongroup RANGE BETWEEN 0 preceding and 1 following) NextGroupKey
 from rad_new_collection_groups_ex1
 where customerkey in (select customerkey from RAD_New_Collection_Groups group by customerkey having count(*)>1)
order by customerkey, collectiongroup

--where customerkey = 8807514



update rad_new_collection_groups
set NextGroupKey = (select max(collectiongroupkey) OVER (partition by customerkey order by collectiongroup RANGE BETWEEN 0 preceding and 1 following) from rad_new_collection_groups x
where x.collectiongroupkey = rad_new_collection_groups.collectiongroupkey)

select * from  RAD_Collection_Group_Disp 
where --customerkey in ((select customerkey from RAD_Collection_Group_Disp group by customerkey having count(*)>2))
customerkey = 8390198
--and 
GROUP_DISPOSITION <> 'Paid'
order by customerkey, collectiongroup

select * from ods.j2_ispcollection
where customerkey = 8390198
order by collectionkey 


Case WHEN