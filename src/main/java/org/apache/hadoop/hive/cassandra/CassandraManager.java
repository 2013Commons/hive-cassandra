package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.thrift.*;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.cassandra.serde.CassandraColumnSerDe;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class to handle the transaction to cassandra backend database.
 *
 */
public class CassandraManager {
  final static public int DEFAULT_REPLICATION_FACTOR = 1;
  final static public String DEFAULT_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";

  //Cassandra Host Name
  private final String host;

  //Cassandra Host Port
  private final int port;

  //Cassandra proxy client
  private CassandraProxyClient clientHolder;

  //Whether or not use framed connection
  private boolean framedConnection;

  //table property
  private final Table tbl;

  //key space name
  private String keyspace;

  //column family name
  private String columnFamilyName;

  /**
   * Construct a cassandra manager object from meta table object.
   */
  public CassandraManager(Table tbl) throws MetaException {
    Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();

    String cassandraHost = serdeParam.get(AbstractCassandraSerDe.CASSANDRA_HOST);
    if (cassandraHost == null) {
      cassandraHost = AbstractCassandraSerDe.DEFAULT_CASSANDRA_HOST;
    }

    this.host = cassandraHost;

    String cassandraPortStr = serdeParam.get(AbstractCassandraSerDe.CASSANDRA_PORT);
    if (cassandraPortStr == null) {
      cassandraPortStr = AbstractCassandraSerDe.DEFAULT_CASSANDRA_PORT;
    }

    try {
      port = Integer.parseInt(cassandraPortStr);
    } catch (NumberFormatException e) {
      throw new MetaException(AbstractCassandraSerDe.CASSANDRA_PORT + " must be a number");
    }

    this.tbl = tbl;
    init();
  }

  private void init() {
    this.keyspace = getCassandraKeyspace();
    this.columnFamilyName = getCassandraColumnFamily();
    this.framedConnection = true;
  }

  /**
   * Open connection to the cassandra server.
   *
   * @throws MetaException
   */
  public void openConnection() throws MetaException {
    try {
      clientHolder =  new CassandraProxyClient(host, port, framedConnection, true);
    } catch (CassandraException e) {
      throw new MetaException("Unable to connect to the server " + e.getMessage());
    }
  }

  /**
   * Close connection to the cassandra server.
   *
   */
  public void closeConnection() {
    if (clientHolder != null) {
      clientHolder.close();
    }
  }

  /**
   * Return a keyspace description for the given keyspace name from the cassandra host.
   *
   * @return keyspace description
   */
  public KsDef getKeyspaceDesc()
    throws NotFoundException, MetaException {
    try {
      return clientHolder.getProxyConnection().describe_keyspace(keyspace);
    } catch (TException e) {
      throw new MetaException("An internal exception prevented this action from taking place."
          + e.getMessage());
    } 
  }

  /**
   * Get Column family based on the configuration in the table. If nothing is found, return null.
   */
  private CfDef getColumnFamily(KsDef ks) {
    for (CfDef cf : ks.getCf_defs()) {
      if (cf.getName().equalsIgnoreCase(columnFamilyName)) {
        return cf;
      }
    }

    return null;
  }

  /**
   * Get CfDef based on the configuration in the table.
   */
  private CfDef getCfDef() throws MetaException {
    CfDef cf = new CfDef();
    cf.setKeyspace(keyspace);
    cf.setName(columnFamilyName);

    cf.setColumn_type(getColumnType());

    return cf;
  }

  /**
   * Create a keyspace with columns defined in the table.
   */
  public KsDef createKeyspaceWithColumns()
    throws MetaException {
    try {
      KsDef ks = new KsDef();
      ks.setName(getCassandraKeyspace());
      ks.setStrategy_class(getStrategy());

      if (!ks.isSetStrategy_options())
        ks.setStrategy_options(new HashMap<String, String>());
      
      ks.putToStrategy_options("replication_factor", Integer.toString(getReplicationFactor()));
    
      ks.addToCf_defs(getCfDef());

      clientHolder.getProxyConnection().system_add_keyspace(ks);
      clientHolder.getProxyConnection().set_keyspace(keyspace);
      return ks;
    } catch (TException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
          + e.getMessage());
    } 
  }

  /**
   * Create the column family if it doesn't exist.
   * @param ks
   * @return
   * @throws MetaException
   */
  public CfDef createCFIfNotFound(KsDef ks) throws MetaException {
    CfDef cf = getColumnFamily(ks);
    if (cf == null) {
      return createColumnFamily();
    } else {
      return cf;
    }
  }

  /**
   * Create column family based on the configuration in the table.
   */
  public CfDef createColumnFamily() throws MetaException {
    CfDef cf = getCfDef();
    try {
      clientHolder.getProxyConnection().set_keyspace(keyspace);
      clientHolder.getProxyConnection().system_add_column_family(cf);
      return cf;
    } catch (TException e) {
      throw new MetaException("Unable to create column family '" + columnFamilyName + "'. Error:"
          + e.getMessage());
    } 
  }

  private String getColumnType() throws MetaException {
    String prop = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING);
    List<String> mapping;
    if (prop != null) {
      mapping = CassandraColumnSerDe.parseColumnMapping(prop);
    } else {
      List<FieldSchema> schema = tbl.getSd().getCols();
      if (schema.size() ==0) {
        throw new MetaException("Can't find table column definitions");
      }

      String[] colNames = new String[schema.size()];
      for (int i = 0; i < schema.size(); i++) {
        colNames[i] = schema.get(i).getName();
      }

      String mappingStr = CassandraColumnSerDe.createColumnMappingString(colNames);
      mapping = Arrays.asList(mappingStr.split(","));
    }

    boolean hasKey = false;
    boolean hasColumn = false;
    boolean hasValue = false;
    boolean hasSubColumn = false;

    for (String column : mapping) {
      if (column.equalsIgnoreCase(CassandraColumnSerDe.CASSANDRA_KEY_COLUMN)) {
          hasKey = true;
      } else if (column.equalsIgnoreCase(CassandraColumnSerDe.CASSANDRA_COLUMN_COLUMN)) {
          hasColumn = true;
      } else if (column.equalsIgnoreCase(CassandraColumnSerDe.CASSANDRA_SUBCOLUMN_COLUMN)) {
        hasSubColumn = true;
      } else if (column.equalsIgnoreCase(CassandraColumnSerDe.CASSANDRA_VALUE_COLUMN)) {
        hasValue = true;
      } else {
        return "Standard";
      }
    }

    if (hasKey && hasColumn && hasValue) {
      if (hasSubColumn) {
        return "Super";
      } else {
        return "Standard";
      }
    } else {
      return "Standard";
    }
  }

  /**
   * Get replication factor from the table property.
   * @return replication factor
   * @throws MetaException error
   */
  private int getReplicationFactor() throws MetaException {
    String prop = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_REPFACTOR);
    if (prop == null) {
      return DEFAULT_REPLICATION_FACTOR;
    } else {
      try {
        return Integer.parseInt(prop);
      } catch (NumberFormatException e) {
        throw new MetaException(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_REPFACTOR + " must be a number");
      }
    }
  }

  /**
   * Get replication strategy from the table property.
   *
   * @return strategy
   */
  private String getStrategy() {
    String prop = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_STRATEGY);
    if (prop == null) {
      return DEFAULT_STRATEGY;
    } else {
      return prop;
    }
  }

  /**
   * Get keyspace name from the table property.
   *
   * @return keyspace name
   */
  private String getCassandraKeyspace() {
    String tableName = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME);

    if (tableName == null) {
      tableName = tbl.getDbName();
    }

    tbl.getParameters().put(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME, tableName);

    return tableName;
  }

  /**
   * Get cassandra column family from table property.
   *
   * @return cassandra column family name
   */
  private String getCassandraColumnFamily() {
    String tableName = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_CF_NAME);

    if (tableName == null) {
      tableName = tbl.getTableName();
    }

    tbl.getParameters().put(AbstractCassandraSerDe.CASSANDRA_CF_NAME, tableName);

    return tableName;
  }

  /**
   * Get the value for a given name from the table.
   * It first checks the table property. If it is not there, it checks the serde properties.
   *
   * @param columnName given name
   * @return value
   */
  private String getPropertyFromTable(String columnName) {
    String prop = tbl.getParameters().get(columnName);
    if (prop == null) {
      prop = tbl.getSd().getSerdeInfo().getParameters().get(columnName);
    }

    return prop;
  }

  /**
   * Drop the table defined in the query.
   */
  public void dropTable() throws MetaException {
    try {
      clientHolder.getProxyConnection().system_drop_column_family(columnFamilyName);
    } catch (TException e) {
      throw new MetaException("Unable to drop column family '" + columnFamilyName + "'. Error:"
          + e.getMessage());
    } 
  }

}
