use spark_process_db;

drop table if exists spark_process_db.retail_summary;

create table spark_process_db.retail_summary
( name  string,city string,total_purch_amt decimal(10,2))
location 'gs://dataprocgcsshell/spark_sql/spark_process.db/';


insert into spark_process_db.retail_summary
with cte1 as 
(
select * 
from 
(select 
customer_id,
cust_name,
city,
grade,
salesman_id
from 
spark_raw_db.customer_lo)
union 
(select 
customer_id,
cust_name,
city,
grade,
salesman_id
from 
spark_raw_db.customer_ny)
),
cte2 as 
(
select 
* 
from 
spark_raw_db.orders 
),
cte3 as (
select 
* 
from 
spark_raw_db.salesman_lo
union 
select 
* 
from 
spark_raw_db.salesman_ny
),
combine as 
(
select 
c3.name as sales_name,
c3.city as sales_city,
round(sum(c2.purch_amt),2) as total_purchase
from 
(select * from cte1) as c1 
join 
(select * from cte2) as c2
on c1.customer_id = c2.customer_id 
join 
(select * from cte3) as c3 
on c3.salesman_id = c2.salesman_id 
group by sales_name,sales_city)
select * from combine order by sales_name;
