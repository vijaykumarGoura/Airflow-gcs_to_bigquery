use spark_raw_db;

create or replace TEMPORARY view customer_ny_v
using csv 
options
(
path='gs://dataprocclustergcs/csv/customer_ny.csv',
header 'true',
inferSchema 'true'
);

drop table if exists customer_ny;
create table if not exists customer_ny
USING PARQUET 
select * from customer_ny_v;
