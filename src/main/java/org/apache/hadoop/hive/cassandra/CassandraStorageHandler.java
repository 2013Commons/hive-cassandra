package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardColumnInputFormat;
import org.apache.hadoop.hive.cassandra.output.HiveCassandraOutputFormat;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.cassandra.serde.CassandraColumnSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraStorageHandler
  implements HiveStorageHandler, HiveMetaHook, HiveStoragePredicateHandler {

  private static final Logger logger = LoggerFactory.getLogger(CassandraStorageHandler.class);

  private Configuration configuration;

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    //Identify Keyspace
    String keyspace = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME);
    if (keyspace == null) {
      keyspace = tableProperties.getProperty(hive_metastoreConstants.META_TABLE_DB);
    }

    jobProperties.put(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME, keyspace);

    //Identify ColumnFamily
    String columnFamily = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_CF_NAME);
    if (columnFamily == null) {
      columnFamily = tableProperties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
    }

    jobProperties.put(AbstractCassandraSerDe.CASSANDRA_CF_NAME, columnFamily);

    //If no column mapping has been configured, we should create the default column mapping.
    String columnInfo = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING);
    if(columnInfo == null)
    {
      columnInfo = CassandraColumnSerDe.createColumnMappingString(
        tableProperties.getProperty(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS));
    }
    jobProperties.put(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING, columnInfo);

    String host = configuration.get(AbstractCassandraSerDe.CASSANDRA_HOST);
    if (host == null) {
      host = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_HOST, AbstractCassandraSerDe.DEFAULT_CASSANDRA_HOST);
    }

    jobProperties.put(AbstractCassandraSerDe.CASSANDRA_HOST, host);

    String port = configuration.get(AbstractCassandraSerDe.CASSANDRA_PORT);
    if (port== null) {
      port = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_PORT, AbstractCassandraSerDe.DEFAULT_CASSANDRA_PORT);
    }

    jobProperties.put(AbstractCassandraSerDe.CASSANDRA_PORT, port);

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_PARTITIONER) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_PARTITIONER,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_PARTITIONER,
          "org.apache.cassandra.dht.Murmur3Partitioner"));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_PARTITIONER,configuration.get(AbstractCassandraSerDe.CASSANDRA_PARTITIONER));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_CONSISTENCY_LEVEL) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_CONSISTENCY_LEVEL,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_CONSISTENCY_LEVEL,
                  AbstractCassandraSerDe.DEFAULT_CONSISTENCY_LEVEL));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_CONSISTENCY_LEVEL,configuration.get(AbstractCassandraSerDe.CASSANDRA_CONSISTENCY_LEVEL));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_RANGE_BATCH_SIZE) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_RANGE_BATCH_SIZE,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_RANGE_BATCH_SIZE,
              Integer.toString(AbstractCassandraSerDe.DEFAULT_RANGE_BATCH_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_RANGE_BATCH_SIZE, configuration.get(AbstractCassandraSerDe.CASSANDRA_RANGE_BATCH_SIZE));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_SIZE) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
              Integer.toString(AbstractCassandraSerDe.DEFAULT_SLICE_PREDICATE_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_SIZE, configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_SIZE));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_SPLIT_SIZE) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SPLIT_SIZE,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_SPLIT_SIZE,
              Integer.toString(AbstractCassandraSerDe.DEFAULT_SPLIT_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SPLIT_SIZE, configuration.get(AbstractCassandraSerDe.CASSANDRA_SPLIT_SIZE));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_BATCH_MUTATION_SIZE) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
              Integer.toString(AbstractCassandraSerDe.DEFAULT_BATCH_MUTATION_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_BATCH_MUTATION_SIZE, configuration.get(AbstractCassandraSerDe.CASSANDRA_BATCH_MUTATION_SIZE));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START, ""));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START, configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH, ""));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH, configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR, ""));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR,
          configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR));
    }

    if (configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED) == null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED,
          tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED, "false"));
    }
    else
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED,
          configuration.get(AbstractCassandraSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED));
    }

    //Set the indexed column names - leave unset if we have problems determining them
    String indexedColumns = tableProperties.getProperty(AbstractCassandraSerDe.CASSANDRA_INDEXED_COLUMNS);
    if (indexedColumns != null)
    {
      jobProperties.put(AbstractCassandraSerDe.CASSANDRA_INDEXED_COLUMNS, indexedColumns);
    }
    else
    {
      try {
        Set<ColumnDef> columns = CassandraPushdownPredicate.getIndexedColumns(host, Integer.parseInt(port), keyspace, columnFamily);
        jobProperties.put(AbstractCassandraSerDe.CASSANDRA_INDEXED_COLUMNS, CassandraPushdownPredicate.serializeIndexedColumns(columns));
      } catch (CassandraException e) {
        // this results in the property remaining unset on the Jobconf, so indexes will not be used on the C* side
        logger.info("Error determining cassandra indexed columns, will not include in JobConf", e);
      }
    }

  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveCassandraStandardColumnInputFormat.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveCassandraOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return CassandraColumnSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  @Override
  public void setConf(Configuration arg0) {
    this.configuration = arg0;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);

    if (!isExternal) {
      throw new MetaException("Cassandra tables must be external.");
    }

    if (table.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for Cassandra.");
    }

    CassandraManager manager = new CassandraManager(table);

    try {
      //open connection to cassandra
      manager.openConnection();
      KsDef ks = manager.getKeyspaceDesc();

      //create the column family if it doesn't exist.
      manager.createCFIfNotFound(ks);
    } catch(NotFoundException e) {
      manager.createKeyspaceWithColumns();
    } finally {
      manager.closeConnection();
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    //TODO: Should this be implemented to drop the table and its data from cassandra
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    if (deleteData && !isExternal) {
      CassandraManager manager = new CassandraManager(table);

      try {
        //open connection to cassandra
        manager.openConnection();
        //drop the table
        manager.dropTable();
      } finally {
        manager.closeConnection();
      }
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    configureTableJobProperties(tableDesc, jobProperties);

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    configureTableJobProperties(tableDesc, jobProperties);

  }

  /**
   * Cassandra requires that an IndexClause must contain at least one IndexExpression with an EQ operator
   * on a configured index column. Other IndexExpression structs may be added to the IndexClause for non-indexed
   * columns to further refine the results of the EQ expression.
   *
   * In order to push down the predicate filtering, we first get a list of indexed columns. If there are no indexed
   * columns, we can't push down the predicate. We then walk down the predicate, and see if there is any filtering that
   * matches the indexed columns. If there is no matching, we can't push down the predicate. For any matching column that
   * is found, we need to verify that there is at least one equal operator. If there is no equal operator, we can't push
   * down the predicate.
   */
  @Override
  public DecomposedPredicate decomposePredicate( JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
    try {
      CassandraColumnSerDe cassandraSerde = (CassandraColumnSerDe) deserializer;
      String host = jobConf.get(AbstractCassandraSerDe.CASSANDRA_HOST, AbstractCassandraSerDe.DEFAULT_CASSANDRA_HOST);
      int port = jobConf.getInt(AbstractCassandraSerDe.CASSANDRA_PORT, Integer.parseInt(AbstractCassandraSerDe.DEFAULT_CASSANDRA_PORT));
      String ksName = cassandraSerde.getCassandraKeyspace();
      String cfName = cassandraSerde.getCassandraColumnFamily();
      Set<ColumnDef> indexedColumns = CassandraPushdownPredicate.getIndexedColumns(host, port, ksName, cfName);
      if (indexedColumns.isEmpty()) {
        return null;
      }

      IndexPredicateAnalyzer analyzer = CassandraPushdownPredicate.newIndexPredicateAnalyzer(indexedColumns);
      List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
      ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicate, searchConditions);

      if (searchConditions.isEmpty()) {
        return null;
      }

      if (!CassandraPushdownPredicate.verifySearchConditions(searchConditions)) {
        return null;
      }

      DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
      decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
      decomposedPredicate.residualPredicate = residualPredicate;

      return decomposedPredicate;
    } catch (CassandraException e) {
      //We couldn't get the indexed column names from Cassandra, return null and let Hive handle the filtering
      logger.info("Error during predicate decomposition", e);
      return null;
    }
  }

}
