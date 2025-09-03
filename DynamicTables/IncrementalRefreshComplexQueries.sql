--Dynamic Tables - Incremental Refresh with Complex Queries

--The following scripts are meant to demonstrate how to create Incremental Dynamic Tables using complex queries that can often lead to Full refreshes.

--Instructions: copy and paste everything below into a Snowflake SQL worksheet and execute the statements after reading the comments.

/*********************************************************************************
Prepare nation table and load some data for testing
**********************************************************************************/
use role sysadmin;
create or replace database dt_demo; 

--Table to hold raw source data. Ideally this would use an insert-only data ingestion approach. 
create or replace table nation (
     n_nationkey number,
     n_name varchar(25),
     n_regionkey number,
     n_comment varchar(152),
     country_code varchar(2),
     update_timestamp timestamp_ntz);
   
--INSERT the raw source data.
--To start, let’s insert 25 rows of data into the NATION table

set update_timestamp = current_timestamp()::timestamp_ntz;
set update_timestamp = dateadd(hour, -1, $update_timestamp); --subtract an hour to mimic data changes over time

begin;
insert into nation values(0,'ALGERIA',0,' haggle. carefully 
final deposits detect slyly agai','DZ',$update_timestamp);
insert into nation values(1,'ARGENTINA',1,'al foxes promise 
slyly according to the regular accounts. bold requests 
alon','AR',$update_timestamp);
insert into nation values(2,'BRAZIL',1,'y alongside of the 
pending deposits. carefully special packages are about the 
ironic forges. slyly special ','BR',$update_timestamp);
insert into nation values(3,'CANADA',1,'eas hang ironic silent 
packages. slyly regular packages are furiously over the tithes. 
fluffily bold','CA',$update_timestamp);
insert into nation values(4,'EGYPT',4,'y above the carefully 
unusual theodolites. final dugouts are quickly across the 
furiously regular d','EG',$update_timestamp);
insert into nation values(5,'ETHIOPIA',0,'ven packages wake 
quickly. regu','ET',$update_timestamp);
insert into nation values(6,'FRANCE',3,'refully final requests. 
regular ironi','FR',$update_timestamp);
insert into nation values(7,'GERMANY',3,'l platelets. regular 
accounts x-ray: unusual regular acco','DE',$update_timestamp);
insert into nation values(8,'INDIA',2,'ss excuses cajole slyly across the packages. deposits print aroun','IN',$update_timestamp);
insert into nation values(9,'INDONESIA',2,' slyly express 
asymptotes. regular deposits haggle slyly. carefully ironic 
hockey players sleep blithely. 
carefull','ID',$update_timestamp);
insert into nation values(10,'IRAN',4,'efully alongside of the 
slyly final dependencies. ','IR',$update_timestamp);
insert into nation values(11,'IRAQ',4,'nic deposits boost atop 
the quickly final requests? quickly 
regula','IQ',$update_timestamp);
insert into nation values(12,'JAPAN',2,'ously. final express 
gifts cajole a','JP',$update_timestamp);
insert into nation values(13,'JORDAN',4,'ic deposits are 
blithely about the carefully regular 
pa','JO',$update_timestamp);
insert into nation values(14,'KENYA',0,' pending excuses haggle 
furiously deposits. pending express pinto beans wake fluffily 
past t','KE',$update_timestamp);
insert into nation values(15,'MOROCCO',0,'rns. blithely bold 
courts among the closely regular packages use furiously bold 
platelets?','MA',$update_timestamp);
insert into nation values(16,'MOZAMBIQUE',0,'s. ironic unusual 
asymptotes wake blithely r','MZ',$update_timestamp);
insert into nation values(17,'PERU',1,'platelets. blithely 
pending dependencies use fluffily across the even pinto beans. 
carefully silent accoun','PE',$update_timestamp);
insert into nation values(18,'CHINA',2,'c dependencies. 
furiously express notornis sleep slyly regular accounts. ideas 
sleep. depos','CN',$update_timestamp);
insert into nation values(19,'ROMANIA',3,'ular asymptotes are 
about the furious multipliers. express dependencies nag above 
the ironically ironic account','RO',$update_timestamp);
insert into nation values(20,'SAUDI ARABIA',4,'ts. silent 
requests haggle. closely express packages sleep across the 
blithely','SA',$update_timestamp);
insert into nation values(21,'VIETNAM',2,'hely enticingly 
express accounts. even final ','VN',$update_timestamp);
insert into nation values(22,'RUSSIA',3,' requests against the 
platelets use never according to the quickly regular 
pint','RU',$update_timestamp);
insert into nation values(23,'UNITED KINGDOM',3,'eans boost 
carefully special requests. accounts are. 
carefull','GB',$update_timestamp);
insert into nation values(24,'UNITED STATES',1,'y final 
packages. slow foxes cajole quickly. quickly silent platelets 
breach ironic accounts. unusual pinto be','US',$update_timestamp);
commit;

--set Timestamp to current time
--Add some changes to data
set update_timestamp = current_timestamp()::timestamp_ntz;
begin;
insert into nation values(5,'ETHIOPIA 2',0,'ven packages wake 
quickly. regu','ET',$update_timestamp);
insert into nation values(15,'MOROCCO 2',0,'rns. blithely bold 
courts among the closely regular packages use furiously bold 
platelets?','MA',$update_timestamp);
commit;

select * from nation order by 1;

/**********************************************************************************************************************
Challenge: DT joining multiple "group by" subqueries results in full refresh
Refresh_Mode_Reason: "Change tracking is not supported on queries with window functions that have disjoint partition keys."
***********************************************************************************************************************/

-----------------------------------------------------------------------------------------------------------------------
--Before creating a new DT, start by writing the query itself and validate that it runs and performs well.
-----------------------------------------------------------------------------------------------------------------------
select
    p.N_NATIONKEY as Nation_Key
    ,p."1" as Nation_Name1
    ,p."2" as Nation_Name2
from (
    select 
        N_NATIONKEY
        ,N_NAME
        ,ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY UPDATE_TIMESTAMP) as row_num
    from nation) n
        pivot (max(N_NAME) for row_num in (1,2)
        ) p
;

-----------------------------------------------------------------------------------------------------------------------
--Notice the PIVOT statement with a single query now can run incrementally .
--the issue comes up when multiple sub-queries or CTEs with "group by" or complex functions get joined.
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE nation_single_query_pivot
TARGET_LAG='DOWNSTREAM'
WAREHOUSE=TOKEN
AS
select
    p.N_NATIONKEY as Nation_Key
    ,p."1" as Nation_Name1
    ,p."2" as Nation_Name2
from (
    select 
        N_NATIONKEY
        ,N_NAME
        ,ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY UPDATE_TIMESTAMP) as row_num
    from nation) n
        pivot (max(N_NAME) for row_num in (1,2)
        ) p
;

show dynamic tables like 'nation_single_query_pivot'; --refresh_mode: INCREMENTAL

-----------------------------------------------------------------------------------------------------------------------
--Now we'll join another subquery that does exactly the same thing
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE nation_multi_query_pivot
TARGET_LAG='DOWNSTREAM'
WAREHOUSE=TOKEN
AS
select
    p.N_NATIONKEY as Nation_Key
    ,p."1" as Nation_Name1
    ,p."2" as Nation_Name2
    ,p2."1" as p2_Nation_Name1
    ,p2."2" as p2_Nation_Name2
from (
    select 
        N_NATIONKEY
        ,N_NAME
        ,ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY UPDATE_TIMESTAMP) as row_num
    from nation) n
        pivot (max(N_NAME) for row_num in (1,2)
        ) p
    join (
    select 
        N_NATIONKEY
        ,N_NAME
        ,ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY UPDATE_TIMESTAMP) as row_num
    from nation) n
        pivot (max(N_NAME) for row_num in (1,2)
        ) p2 on p.N_NATIONKEY = p2.N_NATIONKEY
;
show dynamic tables like 'nation_multi_query_pivot'; --refresh_mode: FULL
-- refresh_mode_reason: Change tracking is not supported on queries with window functions that have disjoint partition keys.

-----------------------------------------------------------------------------------------------------------------------
--Notice the query written without using a PIVOT statement still runs into the same issue
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE nation_groupby_pivot
TARGET_LAG='DOWNSTREAM'
WAREHOUSE=TOKEN
AS
select
    q1_N_NATIONKEY
    ,q1_N_NAME_1
    ,q1_N_NAME_2
    ,q2_N_NAME_1
    ,q2_N_NAME_2
from
(
    select 
        N_NATIONKEY as q1_N_NATIONKEY
        ,max(case when rownum = 1 then N_NAME end) as q1_N_NAME_1
        ,max(case when rownum = 2 then N_NAME end) as q1_N_NAME_2
    from (
            select 
                N_NATIONKEY
                ,N_NAME
                ,row_number () over (partition by N_NATIONKEY order by UPDATE_TIMESTAMP) as rownum
            from nation )
    group by 1
    ) q1
left outer join 
(
    select
        N_NATIONKEY as q2_N_NATIONKEY
        ,max(case when rownum = 1 then N_NAME end) as q2_N_NAME_1
        ,max(case when rownum = 2 then N_NAME end) as q2_N_NAME_2
    from (
        select 
            N_NATIONKEY
            ,N_NAME
            ,row_number () over (partition by N_NATIONKEY order by UPDATE_TIMESTAMP) as rownum
        from nation )
    group by 1
    ) q2 
        on (q1.q1_N_NATIONKEY = q2.q2_N_NATIONKEY)
;

show dynamic tables like 'nation_groupby_pivot'; --refresh_mode: FULL
-- refresh_mode_reason: Change tracking is not supported on queries with window functions that have disjoint partition keys.

-----------------------------------------------------------------------------------------------------------------------
--To get around this split up your complex "group by" queries into separate DTs then join them in a downstream DT
--we'll use the nation_single_query_pivot DT we created earlier and create one more to join later.
-----------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE nation_single_query_pivot2
TARGET_LAG='DOWNSTREAM'
WAREHOUSE=TOKEN
AS
select
    p.N_NATIONKEY as Nation_Key
    ,p."1" as Nation_Name1
    ,p."2" as Nation_Name2
from (
    select 
        N_NATIONKEY
        ,N_NAME
        ,ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY UPDATE_TIMESTAMP) as row_num
    from nation) n
        pivot (max(N_NAME) for row_num in (1,2)
        ) p
;
show dynamic tables like 'nation_single_query_pivot2'; --refresh_mode: INCREMENTAL

--Now create a DT joining the 2 pivot DTs
CREATE OR REPLACE DYNAMIC TABLE nation_multi_query_pivot_fixed
TARGET_LAG='DOWNSTREAM'
WAREHOUSE=TOKEN
AS
select
    p.Nation_Key as q1_N_NATIONKEY
    ,p.Nation_Name1 as q1_N_NAME_1
    ,p.Nation_Name2 as q1_N_NAME_2
    ,p2.Nation_Name1 as q2_N_NAME_1
    ,p2.Nation_Name2 as q2_N_NAME_2
from nation_single_query_pivot p
    join nation_single_query_pivot2 p2 on p.Nation_Key = p2.Nation_Key
;
show dynamic tables like 'nation_multi_query_pivot_fixed'; --refresh_mode: INCREMENTAL

alter dynamic table nation_multi_query_pivot_fixed refresh;

--In the UI you can see the DAG. Notice the lag looks a bit odd in how the final DT has a shorter lag than upstream ones.
--You may wonder why it didn't refresh them since those were set to TARGET_LAG='DOWNSTREAM'

-----------------------------------------------------------------------------------------------------------------------
--Now add some more data to the NATION table
-----------------------------------------------------------------------------------------------------------------------

set update_timestamp = current_timestamp()::timestamp_ntz;
begin;
insert into nation values(5,'ETHIOPIA 3',0,'ven packages wake 
quickly. regu','ET',$update_timestamp);
insert into nation values(15,'MOROCCO 3',0,'rns. blithely bold 
courts among the closely regular packages use furiously bold 
platelets?','MA',$update_timestamp);
commit;
select count(*) from nation;

alter dynamic table nation_multi_query_pivot_fixed refresh;
--Notice the DT does not show that any rows were inserted.
--as it turns out, these rows were dupes and didn't actually change the DT.

--Now insert some new rows
insert into nation values(100,'ETHIOPIA 3',0,'ven packages wake 
quickly. regu','ET',$update_timestamp);

insert into nation values(101,'MOROCCO 3',0,'rns. blithely bold 
courts among the closely regular packages use furiously bold 
platelets?','MA',$update_timestamp);

// Now, you can see inserted rows were added since these keys were not in the DT before.
alter dynamic table nation_multi_query_pivot_fixed refresh; 
show dynamic tables like 'nation_multi_query_pivot_fixed';

