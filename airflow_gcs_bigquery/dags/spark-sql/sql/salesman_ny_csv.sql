use spark_raw_db;

create or replace TEMPORARY view salesman_ny_v
using csv 
options
(
path='gs://dataprocclustergcs/csv/salesman_ny.csv',
header 'true',
inferSchema 'true'
);

drop table if exists salesman_ny;
create table if not exists salesman_ny
USING PARQUET 
select * from salesman_ny_v;
