hive-cassandra
==============

Hive Cassandra storage handler implemented with Hadoop2 and for Apache Shark.


==============

Instructions of usage:

1. Configuration of shark environment.

  a. Download Shark and Hive with hadoop2 from https://groups.google.com/forum/#!topic/shark-users/XXAlk4OACe8.

  b. Choose Prebuild with Hadoop2, CDH4.5.0: shark-0.9.0-hadoop2-bin
                                        AMPLab's Hive 0.11: hive-0.11.0-bin
                                        
  c. Set up shark environment as mentioned in the Shark office page.


2. Install the project:

  a. Git project and compile it.
  b. Copy those jars to the shark(installed dir)/lib/. those jar are : cassandra-all-2.0.4.jar, cassandra-thrift-2.0.4.jar,
     hive-0.11.0-hadoop-2.0.0-cassandra-2.0-0.0.1.jar, whick are in the project/target and project/target/dependency.
     
     
3. execute: in shark directory, and run ./bin/shark.


That's all, and enjoy it.







References:

Hive cassandra serde from project : dvasilen / Hive-Cassandra
https://github.com/dvasilen/Hive-Cassandra/tree/HIVE-0.11.0-HADOOP-2.0.0-CASSANDRA-1.2.9
                          
Cassandra 2.0 hadoop2 from project :  wibiclint / cassandra2-hadoop2
https://github.com/wibiclint/cassandra2-hadoop2                                      
                       
