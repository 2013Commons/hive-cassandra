hive-cassandra
==============

Hive Cassandra Storage Handler implemented with Hadoop2 and for Apache Shark.
The goal of this project is to acquire a Hive Cassandra Storage Handler for Apache Shark, using Cassandra 2.0 and Hadoop 2.0.


==============

Instructions of usage:

1. Configuration of Shark environment.

  a. Download Shark and Hive with Hadoop 2 from https://groups.google.com/forum/#!topic/shark-users/XXAlk4OACe8.

  b. Choose Prebuild with Hadoop2, CDH4.5.0: shark-0.9.0-hadoop2-bin  and   AMPLab's Hive 0.11: hive-0.11.0-bin
                                        
  c. Set up Shark environment as mentioned in the Shark official page.


2. Install the project:

  a. Git project and compile it.
  b. Copy those jars to the shark(installed dir)/lib/. those jar are : cassandra-all-2.0.4.jar, cassandra-thrift-2.0.4.jar,  hive-0.11.0-hadoop-2.0.0-cassandra-2.0-0.0.1.jar, whick are in the project/target and project/target/dependency.
     
     
3. Execute: in shark directory, and run ./bin/shark.


That's all, and enjoy it.

========================================================================================================================
Usage example:

First create table in Cql:

cqlsh>:

create KEYSPACE sharktest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

use sharktest;

CREATE TABLE test(
    id text,
    value text,
    PRIMARY KEY (id))
WITH comment='' AND read_repair_chance = 2.0;

CREATE TABLE test2(
    id text,
    value text,
    PRIMARY KEY (id))
WITH comment='' AND read_repair_chance = 2.0;


INSERT INTO test(id,value)
VALUES ('a','valueA');

INSERT INTO test2(id,value)
VALUES ('a','valueA2');

INSERT INTO test2(id,value)
VALUES ('b','valueB2');

shark>

CREATE EXTERNAL TABLE test
    ( id string,
  value  string) STORED BY
    'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
    WITH SERDEPROPERTIES ("cassandra.ks.name" = "sharktest");

CREATE EXTERNAL TABLE test2
    ( id binary,
  value  string) STORED BY
    'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
    WITH SERDEPROPERTIES ("cassandra.ks.name" = "sharktest");


 A insert query :
 
 shark> INSERT OVERWRITE TABLE test
        SELECT * FROM test2;
        
 A iner join query:
 
 SELECT * FROM test
 INNER JOIN test2
 ON test.id=test2.id;

More example:

CREATE TEMPORARY FUNCTION uuid  as 'org.apache.hadoop.hive.cassandra.ql.udf.UDFUuidToString';
select uuid(key) from log_entries limit 1;

CREATE TEMPORARY FUNCTION fullString  as 'org.apache.hadoop.hive.cassandra.ql.udf.UDFFullString';
select fullString(message) from log_entries limit 1;
// for those string that contains '\n'

example for logs that in Cassandra:

CREATE EXTERNAL TABLE log_entries (
  key string,
  app_name string,
  app_start_time bigint,
  class_name string,
  file_name string,
  host_ip string,
  host_name string,
  level string,
  line_number string,
  log_timestamp bigint,
  logger_name string,
  message string,
  method_name string,
  ndc string,
  thread_name string,
  throwable_str_rep string
)  STORED BY
    'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
    WITH SERDEPROPERTIES (
"cassandra.ks.name" = "log_user_usage_history",
"compression"="LZ4Compressor"
) ;

create table log_cache TBLPROPERTIES ("shark.cache" = "true") AS SELECT uuid(key) from log_entries;




========================================================================================================================
References:

Hive cassandra serde from project : dvasilen / Hive-Cassandra
https://github.com/dvasilen/Hive-Cassandra/tree/HIVE-0.11.0-HADOOP-2.0.0-CASSANDRA-1.2.9
                          
Cassandra 2.0 hadoop2 from project :  wibiclint / cassandra2-hadoop2
https://github.com/wibiclint/cassandra2-hadoop2                                      
                       
Shark and Hive with hadoop2 : https://groups.google.com/forum/#!topic/shark-users/XXAlk4OACe8.
