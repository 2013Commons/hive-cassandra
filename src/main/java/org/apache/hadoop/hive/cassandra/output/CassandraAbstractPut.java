package org.apache.hadoop.hive.cassandra.output;

import org.apache.cassandra.thrift.*;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class CassandraAbstractPut implements Put {

  /**
   * Parse batch mutation size from job configuration. If none is defined, return the default value 500.
   *
   * @param jc job configuration
   * @return batch mutation size
   */
  protected int getBatchMutationSize(JobConf jc) {
    return jc.getInt(
            AbstractCassandraSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
            AbstractCassandraSerDe.DEFAULT_BATCH_MUTATION_SIZE);
  }

  /**
   * Parse consistency level from job configuration. If none is defined,  or if the specified value is not a valid
   * <code>ConsistencyLevel</code>, return default consistency level ONE.
   *
   * @param jc job configuration
   * @return cassandra consistency level
   */
  protected static ConsistencyLevel getConsistencyLevel(JobConf jc) {
    String consistencyLevel = jc.get(AbstractCassandraSerDe.CASSANDRA_CONSISTENCY_LEVEL,
            AbstractCassandraSerDe.DEFAULT_CONSISTENCY_LEVEL);
    ConsistencyLevel level = null;
    try {
      level = ConsistencyLevel.valueOf(consistencyLevel);
    } catch (IllegalArgumentException e) {
      level = ConsistencyLevel.ONE;
    }

    return level;
  }

  /**
   * Commit the changes in mutation map to cassandra client for given keyspace with given consistency level.
   *
   * @param keySpace cassandra key space
   * @param client cassandra client
   * @param flevel cassandra consistency level
   * @param mutation_map cassandra mutation map
   * @throws IOException when error happens in batch mutate
   */
  protected void commitChanges(String keySpace,
      CassandraProxyClient client,
      ConsistencyLevel flevel,
      Map<ByteBuffer, Map<String,List<Mutation>>> mutation_map) throws IOException {
    try {
      client.getProxyConnection().set_keyspace(keySpace);
      client.getProxyConnection().batch_mutate(mutation_map, flevel);
    } catch (InvalidRequestException e) {
      throw new IOException(e);
    } catch (UnavailableException e) {
      throw new IOException(e);
    } catch (TimedOutException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
