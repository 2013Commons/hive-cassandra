example1:
CQL:
CREATE KEYSPACE metrics WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 2}; 
USE metrics; 
CREATE TABLE metric (  
    api varchar,
    name varchar,
    time timestamp,
    value double,
    PRIMARY KEY (api, name, time)
);

INSERT INTO metric(api, name, time, value) VALUES('my_api', 'cpu0', dateof(NOW()), 0.31); 

SHARK:

shark> CREATE DATABASE metrics;  
shark> USE metrics;  

shark> CREATE EXTERNAL TABLE metric (api string, name string, time timestamp, value double) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler' WITH SERDEPROPERTIES("cassandra.cf.name" = "metric","cassandra.host"="127.0.0.1","cassandra.port" = "9160") TBLPROPERTIES ("cassandra.ks.name" = "metrics"); 
 
shark> select * from metric;  
shark> select avg(value) from metric where api = 'my_api' and name = 'cpu0'; 



example2
CQL:
create KEYSPACE sharktest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
use sharktest;

CREATE TABLE test( id text, value text, PRIMARY KEY (id)) WITH comment='' AND read_repair_chance = 2.0;

CREATE TABLE test2( id text, value text, PRIMARY KEY (id)) WITH comment='' AND read_repair_chance = 2.0;

INSERT INTO test(id,value) VALUES ('a','valueA');

INSERT INTO test2(id,value) VALUES ('a','valueA2');

INSERT INTO test2(id,value) VALUES ('b','valueB2');

SHARK:
CREATE EXTERNAL TABLE test ( id string, value string) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler' WITH SERDEPROPERTIES ("cassandra.ks.name" = "sharktest");

CREATE EXTERNAL TABLE test2 ( id binary, value string) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler' WITH SERDEPROPERTIES ("cassandra.ks.name" = "sharktest");

insert query :

shark> INSERT OVERWRITE TABLE test SELECT * FROM test2;

iner join query:

SELECT * FROM test t INNER JOIN test2 test2 ON t.id=cast(test2.id as string);


//create keyspace and table definition from hive to cassandra

CREATE EXTERNAL TABLE test3 (a string, name string, time timestamp) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler' WITH SERDEPROPERTIES("cassandra.cf.name" = "test3","cassandra.host"="127.0.0.1","cassandra.port" = "9160") TBLPROPERTIES ("cassandra.ks.name" = "sharktest");
// field a will be the primary key

create table test4(a text primary key, b MAP<int,int>);
INSERT INTO test4(a,b) VALUES ('a',{1:10}); 

//shark
create external table test4 (a string, b MAP<int,int>)  STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler' WITH SERDEPROPERTIES("cassandra.cf.name" = "test4","cassandra.host"="127.0.0.1","cassandra.port" = "9160") TBLPROPERTIES ("cassandra.ks.name" = "sharktest");

select * from test4; // not working




