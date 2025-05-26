use spark_raw_db;

create or replace TEMPORARY view salesman_lo_v
using csv 
options
(
path='gs://dataprocclustergcs/csv/salesman_lo.csv',
header 'true',
inferSchema 'true'
);

drop table if exists salesman_lo;
create table if not exists salesman_lo
USING PARQUET 
select * from salesman_lo_v;
