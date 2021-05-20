use proj1db;

show databases;

show tables;
select * from bevcountc;

--create tables for bev branch  and bevcount
create table if not exists BevBranchA (beverage STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BevBranchB (beverage STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BevBranchC (beverage STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BevCountA (beverage STRING, count INT) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BevCountB (beverage STRING, count INT) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BevCountC (beverage STRING, count INT) row format delimited fields terminated by ',' stored as textfile;

--load data from HDFS into hive
LOAD DATA INPATH '/user/arielrubio/projectdata/Bev_BranchA.txt' OVERWRITE INTO TABLE bevbranchA;
LOAD DATA INPATH '/user/arielrubio/projectdata/Bev_BranchB.txt' OVERWRITE INTO TABLE bevbranchB;
LOAD DATA INPATH '/user/arielrubio/projectdata/Bev_BranchC.txt' OVERWRITE INTO TABLE bevbranchC;
LOAD DATA INPATH '/user/arielrubio/projectdata/Bev_ConscountA.txt' OVERWRITE INTO TABLE bevcountA;
LOAD DATA INPATH '/user/arielrubio/projectdata/Bev_ConscountB.txt' OVERWRITE INTO TABLE bevcountB;
LOAD DATA INPATH '/user/arielrubio/projectdata/Bev_ConscountC.txt' OVERWRITE INTO TABLE bevcountC;

-- Create tables for problem scenario 1
create table branch1 as select * from bevbrancha where branch = 'Branch1';
insert into table branch1 select * from bevbranchb where branch = 'Branch1';
insert into table branch1 select * from bevbranchc where branch = 'Branch1';


select * from bevbrancha where branch = 'Branch1';
select * from bevbranchb where branch = 'Branch1';
select * from bevbranchc where branch = 'Branch1';
create table countbranch1(beverage string, count bigint);
insert into table 
countbranch1 (select BevcountA.beverage,sum(BevcountA.count) from branch1 join BevcountA on(branch1.beverage = BevcountA.beverage ) group by BevcountA.beverage 
union all
select BevcountB.beverage,sum(BevcountB.count) from branch1 join BevcountB on(branch1.beverage = BevcountB.beverage ) group by BevcountB.beverage
union all
select BevcountC.beverage,sum(BevcountC.count) from branch1 join BevcountC on(branch1.beverage = BevcountC.beverage ) group by BevcountC.beverage);



--1a)What is the total number of consumers for Branch1?
select sum(count) as consumer_total from countbranch1;

--1b)What is the total number of consumers for Branch2?
select sum(count) as consumer_total from countbranch2;

--2a)What is the most consumed beverage on Branch1
select beverage, sum(count) totalsum from countbranch1 group by beverage order by totalsum desc limit 1;

--2b)What is the least consumed beverage on Branch2
select beverage, sum(count) totalsum from countbranch2 group by beverage order by totalsum limit 1;

--3a)What are the beverages avilable on Branch 10, Branch 8, Branch 1?
--make a table that combines all the branch tables (a,b,c)
create table branch_all as select * from bevbrancha;
insert into table branch_all (select * from bevbranchb);
insert into table branch_all (select * from bevbranchc);
select * from branch_all;

select distinct * from branch_all where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';

--3b)What are the common beverages available in Branch4, Branch7?
--select distinct * from branch_all where branch = 'Branch4' or branch = 'Branch7';
select distinct * from branch_all b1
inner join branch_all b2 on b1.beverage = b2.beverage and (b1.branch = 'Branch4' and b2.branch = 'Branch7')
order by b1.beverage;
--4) Create a partition, index, view for the scenario 3. 
--partition
CREATE TABLE branch_all_partition(
	beverage STRING
) PARTITIONED BY (branch STRING)
row format delimited fields terminated by ',' stored as textfile;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table branch_all_partition PARTITION(branch) select beverage, branch from branch_all;

select * from branch_all_partition;

--3a partition query
select distinct * from branch_all where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
--3b partition query
select distinct * from branch_all_partition b1
inner join branch_all_partition b2 on b1.beverage = b2.beverage and (b1.branch = 'Branch4' and b2.branch = 'Branch7')-- and (b1.branch <> b2.branch)
order by b1.beverage;
--index
Create INDEX beverage_index on table branch_all(beverage) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
select distinct * from branch_all where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
--view
CREATE view branch10_8_1 as select distinct * from branch_all where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
select * from branch10_8_1;

--5) Alter the table properties to add "note", "comment"
ALTER TABLE branch_all SET TBLPROPERTIES ('note' = 'This combines the bevbranchA, bevbranchB, and bevBranchC text files of all the branches.');
ALTER TABLE branch_all SET TBLPROPERTIES ('comment' = 'This table property is a comment.');
show tblproperties branch_all;
--6) Remove the row 5 from the output of scenario 1
create table delete5row as (select distinct * from branch_all where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1' order by beverage);
select beverage, branch, 
ROW_NUMBER() OVER (ORDER BY beverage) as rank
from delete5row;

INSERT OVERWRITE TABLE delete5row
Select tmp.beverage, tmp.branch
from (select beverage, branch, 
	ROW_NUMBER() OVER (ORDER BY beverage) as rank 
	from delete5row) as tmp
WHERE tmp.rank != 5;

select beverage, branch, 
ROW_NUMBER() OVER (ORDER BY beverage) as rank
from delete5row;
