Hive-Cassandra
==============

1. Hive Storage Handler for Cassandra (cloned from https://github.com/riptano/hive/tree/hive-0.8.1-merge/cassandra-handler)
2. git pull https://github.com/milliondreams/hive.git cas-support-cql
3. git pull https://github.com/michaelsembwever/cassandra-hadoop
4. Updated for Hive 0.11.0 Hadoop 2.0 and and Cassandra 1.2.9

Cassandra CLI Example
==============

./cassandra-cli

create keyspace test
with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'
and strategy_options = [{replication_factor:1}];

create column family users with comparator = UTF8Type;

update column family users with
        column_metadata =
        [
        {column_name: first, validation_class: UTF8Type},
        {column_name: last, validation_class: UTF8Type},
        {column_name: age, validation_class: UTF8Type, index_type: KEYS}
        ];

assume users keys as utf8;

set users['jsmith']['first'] = 'John';
set users['jsmith']['last'] = 'Smith';
set users['jsmith']['age'] = '38';
set users['jdoe']['first'] = 'John';
set users['jdoe']['last'] = 'Dow';
set users['jdoe']['age'] = '42';

get users['jdoe'];

./hive 

DROP TABLE IF EXISTS cassandra_users;

CREATE EXTERNAL TABLE cassandra_users  (key string, first string, last string, age string)
STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES("cassandra.cf.name" = "users","cassandra.host"="&lt;your_host&gt;","cassandra.port" = "9160")
TBLPROPERTIES ("cassandra.ks.name" = "test");

select * from cassandra_users;

See http://www.datastax.com/docs/datastax_enterprise3.0/solutions/about_hive for other SERDE and TABLE properties.


Cassandra CQL Example
==============

./cqlsh

DESCRIBE keyspaces;

CREATE KEYSPACE TEST WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

USE TEST;

CREATE TABLE bankloan_10(
  row int,
  age text,
  ed text,
  employ text,
  address text,
  income text,
  debtinc text,
  creddebt text,
  othdebt text,
  default text,
  PRIMARY KEY(row)
);


INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (1,'41','3','17','12','176','9.3','11.359392','5.008608','1');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (2,'27','1','10','6','31','17.3','1.362202','4.000798','0');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (3,'40','1','15','14','55','5.5','0.856075','2.168925','0');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (4,'41','1','15','14','120','2.9','2.65872','0.82128','0');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (5,'24','2','2','0','28','17.3','1.787436','3.056564','1');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (6,'41','2','5','5','25','10.2','0.3927','2.1573','0');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (7,'39','1','20','9','67','30.6','3.833874','16.668126','0');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (8,'43','1','12','11','38','3.6','0.128592','1.239408','0');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (9,'24','1','3','4','19','24.4','1.358348','3.277652','1');
INSERT INTO bankloan_10 (row, age,ed,employ,address,income,debtinc,creddebt,othdebt,default)   VALUES (10,'36','1','0','13','25','19.7','2.7777','2.1473','0');

SELECT * FROM bankloan_10

./hive

DROP TABLE IF EXISTS cassandra_bankloan_10;

CREATE EXTERNAL TABLE cassandra_bankloan_10 (row int, age string,ed string,employ string,address string,income string,debtinc string,creddebt string,othdebt string,default string)
STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES("cassandra.cf.name" = "bankloan_10","cassandra.host"="&lt;your_host&gt;","cassandra.port" = "9160")
TBLPROPERTIES ("cassandra.ks.name" = "test");

select * from cassandra_bankloan_10;

See http://www.datastax.com/docs/datastax_enterprise3.1/solutions/about_hive for other SERDE and TABLE properties.
