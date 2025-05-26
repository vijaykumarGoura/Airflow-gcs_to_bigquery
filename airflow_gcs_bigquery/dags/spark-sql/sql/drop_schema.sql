/*drop database*/

drop schema spark_raw_db cascade;
create database if not exists spark_raw_db
location 'gs://dataprocgcsshell/spark_sql/spark_raw.db';

drop schema spark_process_db cascade;
create database if not exists spark_process_db
location 'gs://dataprocgcsshell/spark_sql/spark_process.db';
