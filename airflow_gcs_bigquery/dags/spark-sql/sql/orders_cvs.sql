use spark_raw_db;

create or replace TEMPORARY view orders_v
using csv 
options
(
path='gs://dataprocclustergcs/csv/orders.csv',
header 'true',
inferSchema 'true'
);


create table if not exists orders
USING PARQUET 
select * from orders_v;
