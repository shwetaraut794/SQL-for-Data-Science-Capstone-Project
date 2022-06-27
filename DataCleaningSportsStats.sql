--Query to remove unwanted characters from name in Athlete
select REPLACE(REPLACE(REPLACE( REPLACE(name, ')', ''),'(',''),'-',' '),'"','') from athlete;

update Athlete 
set name = REPLACE(REPLACE(REPLACE( REPLACE(name, ')', ''),'(',''),'-',' '),'"','');

--Query to trim unwanted spaces 
select len(trim(name)), len(name) from athlete where len(trim(name)) <> len(name);

update athlete 
SET name = TRIM(name);

Update Athlete
SET name = REPLACE(name,'  ',' ') from athlete;

--Query to convert Age to int in order to perform any mathematical operations if required for analysis
update athlete
SET age = ROUND(Age, 0)
where age <> 'NaN';

update athlete 
SET age = NULL 
where age = 'NaN';

alter table athlete
alter column age int;

--Query to convert Weight & height to Float in order to perform any mathematical operations if required for analysis
select Weight, ROUND(Weight, 0) from athlete where Weight <> 'Nan' and Weight <> ROUND(Weight,0);

update athlete 
SET Weight = NULL 
where Weight = 'NaN';

alter table athlete
alter column Weight Float;

update athlete 
SET Height = NULL 
where Height = 'NaN';

alter table athlete
alter column Height Float;


update athlete SET Medal = NULL where Medal = 'Nan';

--Query to check if there's a need to trim column Events
select len(trim(Events)), len(Events) from Event where len(trim(Events)) <> len(Events);

select distinct(events) from Event where events like '%Basketball';
select * from event

Select * from NOC where Region like 'China%'
Select * from Team

--Query to fill value in region where region is 'Nan'
select * from NOC n join Team t
on n.NOC_id = t.NOC_id
where n.NOC_id = t.NOC_id
and n.Region ='NaN'

update n 
SET n.Region = t.Teams
from NOC n join Team t
on n.NOC_id = t.NOC_id
where n.NOC_id = t.NOC_id
and n.Region ='NaN'

select * from eventdetail
