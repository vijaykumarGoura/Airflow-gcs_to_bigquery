use spark_raw_db;

create or replace TEMPORARY view customer_lo_v
using csv 
options
(
path='gs://dataprocclustergcs/csv/customer_lo.csv',
header 'true',
inferSchema 'true'
);

drop table if exists customer_lo;

create  table if not exists customer_lo
USING PARQUET 
select * from customer_lo_v;
