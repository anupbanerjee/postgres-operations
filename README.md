# postgres-operations
Postgres Sample
Step 1) Create a database in Postgres assign owner as Postgres - 

CREATE DATABASE cmts_ipdr_data OWNER postgres;

Step 2) Create table as 

	create table ipdrdata (
		 cmts_id int,
		 ipaddress varchar,
		 timeinmillis bigint,
		 upstream bigint,
		 downstream bigint,
		 primary key(cmts_id, ipaddress, timeinmillis)
	)

Step 3) Application usage

"-h 127.0.01 -p 5432 -u postgres -pw postgres"