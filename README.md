hive-cassandra
==============

Hive Cassandra Storage Handler implemented with Hadoop2 and for Apache Shark.
The goal of this project is to acquire a Hive Cassandra Storage Handler for Apache Shark, using Cassandra 2.0 and Hadoop 2.0.


==============

Instructions of usage:

1. Configuration of Shark environment.

  a. Download Shark and Hive with Hadoop 2 from https://groups.google.com/forum/#!topic/shark-users/XXAlk4OACe8.

  b.  Choose Prebuild with Hadoop2, CDH4.5.0:
  
      shark-0.9.0-hadoop2-bin:  
  
      https://www.google.com/url?q=https%3A%2F%2Fdl.dropboxusercontent.com%2Fs%2Fzya05f11hohznzw%2Fshark-0.9.0-hadoop2-bin.tgz%3Fdl%3D1%26token_hash%3DAAEjSNRvO4KmXnRMnJptoifwMJTVw8m2uCtIhpq3-XADhg&sa=D&sntz=1&usg=AFQjCNFhmRRzq3m2YPsL78KeQqPzUGicQQ
      
      AMPLab's Hive 0.11: 
      
      hive-0.11.0-bin
      
      https://www.google.com/url?q=https%3A%2F%2Fdl.dropboxusercontent.com%2Fs%2Fiofpitbs9tiju1s%2Fhive-0.11.0-bin.tgz%3Fdl%3D1%26token_hash%3DAAGKUddKHYFIAYzQSvRvsAPoykPGFo1YozBznqMhI66Lyg&sa=D&sntz=1&usg=AFQjCNGe6ROWW74BbSNIhEwJqr7TvJRlnw
                                        
  c. Set up Shark environment as mentioned in the Shark official page (scala-2.10.3 and shark-env.sh).


2. Install the project:

  a. Git project and compile it.
  
  b. Copy those jars to the shark(installed dir)/lib/.
  
    Those jar are : cassandra-all-2.0.4.jar, 
                    cassandra-thrift-2.0.4.jar,  
                    hive-0.11.0-hadoop-2.0.0-cassandra-2.0-0.0.1.jar,
                    
    whick are in the project/target and project/target/dependency.
     
     
3. Execute: in shark directory, and run ./bin/shark.


That's all, and enjoy it.

========================================================================================================================


Usage example 1:

First create table in Cql:

cqlsh>:

create KEYSPACE sharktest WITH 
replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

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

==============================================================================================

Example with application log that in Cassandra:

CREATE TEMPORARY FUNCTION uuid
as 'org.apache.hadoop.hive.cassandra.ql.udf.UDFUuidToString';

// pass binary to string

select uuid(key) from log_entries limit 1;



CREATE TEMPORARY FUNCTION fullString  
as 'org.apache.hadoop.hive.cassandra.ql.udf.UDFFullString';

// for those string that contains '\n'

select fullString(message) from log_entries limit 1;



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
)  STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
    WITH SERDEPROPERTIES (
"cassandra.ks.name" = "log_user_usage_history",
"compression"="LZ4Compressor"

) ;


//cache log table

create table log_cache TBLPROPERTIES ("shark.cache" = "true") 
AS SELECT uuid(key), fullstring(message) from log_entries;


Queries:

select count(*) from log_cache;

OK

148081

Time taken: 2.38 seconds



Export logs from cassandra:

create table log_cache (key string, log string);

insert into table log_cache  SELECT uuid(key), fullstring(message) from log_entries;


select count(*) from log_cache where log like '%createUser%';

OK

130640

Time taken: 3.374 seconds


select * from log_cache where log like '%ahll11%' and log like '%getContent%' limit 1;

bbc6ef58-7f4b-310d-8689-64130b077231	[83.34.124.168] [/getContentList]    ***********

select count(*) from log_cache where log like '%ahll11%' and log like '%getContent%';

OK

336

Time taken: 3.697 seconds


That is all the example of the usage of Shark for those logs that in cassandra. 



========================================================================================================================
References:

Hive cassandra serde from project : dvasilen / Hive-Cassandra
https://github.com/dvasilen/Hive-Cassandra/tree/HIVE-0.11.0-HADOOP-2.0.0-CASSANDRA-1.2.9
                          
Cassandra 2.0 hadoop2 from project :  wibiclint / cassandra2-hadoop2
https://github.com/wibiclint/cassandra2-hadoop2                                      
                       
Shark and Hive with hadoop2 : https://groups.google.com/forum/#!topic/shark-users/XXAlk4OACe8.
