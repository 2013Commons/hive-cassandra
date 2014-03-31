package org.apache.hadoop.hive.cassandra.cql;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.CassandraClientHolder;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.cassandra.serde.cql.CqlSerDe;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A class to handle the transaction to cassandra backend database.
 */
public class CqlManager {

    final static public int DEFAULT_REPLICATION_FACTOR = 1;
    final static public String DEFAULT_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    private static final Logger logger = LoggerFactory.getLogger(CqlManager.class);

    final static Map<String, String> hiveTypeToCqlType = new HashMap<String, String>();

    static {
        hiveTypeToCqlType.put(org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME, "text");
        hiveTypeToCqlType.put(org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME, "int");
        hiveTypeToCqlType.put(org.apache.hadoop.hive.serde.Constants.BOOLEAN_TYPE_NAME, "boolean");
        hiveTypeToCqlType.put(org.apache.hadoop.hive.serde.Constants.DOUBLE_TYPE_NAME, "double");
        hiveTypeToCqlType.put(org.apache.hadoop.hive.serde.Constants.FLOAT_TYPE_NAME, "float");
    }

    //Cassandra Host Name
    private final String host;

    //Cassandra Host Port
    private final int port;

    //Cassandra proxy client
    private CassandraClientHolder cch;

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
    public CqlManager(Table tbl) throws MetaException {
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
            cch = new CassandraProxyClient(host, port, framedConnection, true).getClientHolder();
        } catch (CassandraException e) {
            throw new MetaException("Unable to connect to the server " + e.getMessage());
        }
    }

    /**
     * Close connection to the cassandra server.
     */
    public void closeConnection() {
        if (cch != null) {
            cch.close();
        }
    }

    public boolean doesKeyspaceExist() throws MetaException {
        String getKeyspaceQuery = "select * from system.schema_keyspaces where keyspace_name='%s'";
        try {
            CqlResult result = cch.getClient().execute_cql3_query(ByteBufferUtil.bytes(String.format(getKeyspaceQuery, keyspace)), Compression.NONE, ConsistencyLevel.ONE);
            List<CqlRow> rows = result.getRows();
            //there can be only be one keyspace with the given name or no keyspace at all
            assert rows.size() <= 1;
            return rows.size() == 1;
        } catch (InvalidRequestException e) {
            throw new MetaException("Unable to create keyspace " + keyspace + ". Error:" + e.getWhy());
        } catch (Exception e) {
            throw new MetaException("Unable to create keyspace " + keyspace + ". Error:" + e.getMessage());
        }
    }

    public void createKeyspace() throws MetaException {
        String createKeyspaceQuery = "create keyspace %s WITH replication = { 'class' : %s, %s } AND durable_writes = %s";
        String durableWrites = getPropertyFromTable(AbstractCassandraSerDe.DURABLE_WRITES);
        if (durableWrites == null) {
            durableWrites = "true";
        }
        String strategy = "'" + getStrategy() + "'";
        String query = String.format(createKeyspaceQuery, keyspace, strategy, getStrategyOptions(), durableWrites);
        try {
            cch.getClient().execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            throw new MetaException("Unable to create keyspace '" + keyspace + "'. Error:" + e.getWhy());
        } catch (Exception e) {
            throw new MetaException("Unable to create keyspace '" + keyspace + "'. Error:" + e.getMessage());
        }
    }

    public String getStrategyOptions() throws MetaException {
        String replicationFactor = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_REPFACTOR);
        String strategyOptions = getPropertyFromTable(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_STRATEGY_OPTIONS);
        if (replicationFactor != null) {
            if (strategyOptions != null) {
                throw new MetaException("Unable to create keyspace '" + keyspace + "' Specify only one of 'cassandra.ks.repfactor' or 'cassandra.ks.stratOptions'");
            }
            return "'replication_factor' : " + replicationFactor;
        }
        if (strategyOptions == null) {
            throw new MetaException("Unable to create keyspace '" + keyspace + "' Specify either 'cassandra.ks.repfactor' or 'cassandra.ks.stratOptions'");
        }
        return strategyOptions;
    }

    /**
     * Create the column family if it doesn't exist.
     *
     * @return
     * @throws MetaException
     */
    public void createCFIfNotFound() throws MetaException {
        boolean cfExists = checkColumnFamily();
        if (!cfExists) {
            createColumnFamily();
        }
    }

    private boolean checkColumnFamily() throws MetaException {
        boolean cfExists = false;

        String getCFQuery = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='%s';";
        String finalQuery = String.format(getCFQuery, keyspace);
        try {
            CqlResult colFamilies = cch.getClient().execute_cql3_query(ByteBufferUtil.bytes(finalQuery), Compression.NONE, ConsistencyLevel.ONE);
            List<CqlRow> rows = colFamilies.getRows();

            for (CqlRow row : rows) {
                String cfName = new String(row.columns.get(0).getValue());
                if (columnFamilyName.equalsIgnoreCase(cfName)) {
                    cfExists = true;
                }
            }
        } catch (UnavailableException e) {
            MetaException me = new MetaException(e.getMessage());
            me.setStackTrace(e.getStackTrace());
            throw me;
        } catch (TException e) {
            MetaException me = new MetaException(e.getMessage());
            me.setStackTrace(e.getStackTrace());
            throw me;
        }
        return cfExists;
    }

    /**
     * Create column family based on the configuration in the table.
     */
    public void createColumnFamily() throws MetaException {
        try {
            cch.getClient().set_keyspace(keyspace);
            Properties properties = MetaStoreUtils.getTableMetadata(tbl);

            String columnsStr = (String) properties.get(hive_metastoreConstants.META_TABLE_COLUMNS);
            String columnTypesStr = (String) properties.get(hive_metastoreConstants.META_TABLE_COLUMN_TYPES);

            String[] columnNames = columnsStr.split(",");
            String[] columnTypes = columnTypesStr.split(":");
            if (columnNames.length != columnTypes.length) {
                throw new MetaException("Unable to create column family '" + columnFamilyName + ". Error: Column names count and column types count do not match");
            }

            StringBuilder queryBuilder = new StringBuilder("CREATE TABLE ");
            queryBuilder.append(keyspace);
            queryBuilder.append(".");
            queryBuilder.append(columnFamilyName);
            queryBuilder.append("(");
            for (int i = 0; i < columnNames.length; i++) {
                queryBuilder.append(columnNames[i]);
                queryBuilder.append(" ");
                queryBuilder.append(hiveTypeToCqlType.get(columnTypes[i]));
                queryBuilder.append(",");
            }
            String keyStr = getPropertyFromTable(CqlSerDe.CASSANDRA_COLUMN_FAMILY_PRIMARY_KEY);
            if (keyStr == null || keyStr.isEmpty()) {
                keyStr = columnNames[0];
            }
            queryBuilder.append(" primary key (");
            queryBuilder.append(keyStr);
            queryBuilder.append(")");
            queryBuilder.append(")");

            Map<String, String> options = constructTableOptions();
            if (!options.isEmpty()) {
                queryBuilder.append(" WITH ");
                Iterator<Map.Entry<String, String>> optionIterator = options.entrySet().iterator();
                while (optionIterator.hasNext()) {
                    Map.Entry<String, String> entry = optionIterator.next();

                    queryBuilder.append(entry.getKey());

                    queryBuilder.append(" = ");

                    queryBuilder.append(entry.getValue());

                    if (optionIterator.hasNext()) {
                        queryBuilder.append(" AND ");
                    }
                }
            }

            cch.getClient().execute_cql3_query(ByteBufferUtil.bytes(queryBuilder.toString()), Compression.NONE, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new MetaException("Unable to create column family '" + columnFamilyName + "'. Error:"
                    + e.getMessage());
        }

    }

    private Map<String, String> constructTableOptions() {

        Map<String, String> options = new HashMap<String, String>();
        addIfNotEmpty(AbstractCassandraSerDe.COLUMN_FAMILY_COMMENT, options, true);
        addIfNotEmpty(AbstractCassandraSerDe.READ_REPAIR_CHANCE, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.DCLOCAL_READ_REPAIR_CHANCE, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.GC_GRACE_SECONDS, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.BLOOM_FILTER_FP_CHANCE, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.COMPACTION, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.COMPRESSION, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.REPLICATE_ON_WRITE, options, false);
        addIfNotEmpty(AbstractCassandraSerDe.CACHING, options, false);

        return options;
    }

    private void addIfNotEmpty(String property, Map<String, String> options, boolean wrapQuotes) {
        String temp = getPropertyFromTable(property);
        if (wrapQuotes) {
            temp = "'" + temp + "'";
        }
        if (temp != null && !temp.isEmpty()) {
            options.put(property, temp);
        }
    }

    /**
     * Get replication factor from the table property.
     *
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
     * Get the value for a given name from the table. It first checks the table
     * property. If it is not there, it checks the serde properties.
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
            cch.getClient().system_drop_column_family(columnFamilyName);
        } catch (TException e) {
            throw new MetaException("Unable to drop column family '" + columnFamilyName + "'. Error:"
                    + e.getMessage());
        }
    }

}
