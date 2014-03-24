package org.apache.hadoop.hive.cassandra.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public abstract class AbstractCassandraSerDe implements SerDe{

    public static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraSerDe.class);

    public static final String CASSANDRA_KEYSPACE_NAME = "cassandra.ks.name"; // keyspace
    public static final String CASSANDRA_KEYSPACE_REPFACTOR = "cassandra.ks.repfactor"; //keyspace replication factor
    public static final String CASSANDRA_KEYSPACE_STRATEGY = "cassandra.ks.strategy"; //keyspace replica placement strategy
    public static final String CASSANDRA_KEYSPACE_STRATEGY_OPTIONS = "cassandra.ks.stratOptions";
    public static final String DURABLE_WRITES = "durable.writes";

    public static final String CASSANDRA_CF_NAME = "cassandra.cf.name"; // column family
    public static final String CASSANDRA_RANGE_BATCH_SIZE = "cassandra.range.size";
    public static final String CASSANDRA_SLICE_PREDICATE_SIZE = "cassandra.slice.predicate.size";
    public static final String CASSANDRA_SPLIT_SIZE = "cassandra.input.split.size";
    public static final String CASSANDRA_HOST = "cassandra.host"; // initialHost
    public static final String CASSANDRA_PORT = "cassandra.port"; // rcpPort
    public static final String CASSANDRA_PARTITIONER = "cassandra.partitioner"; // partitioner
    public static final String CASSANDRA_COL_MAPPING = "cassandra.columns.mapping";
    public static final String CASSANDRA_INDEXED_COLUMNS = "cassandra.indexed.columns";

    public static final String CASSANDRA_BATCH_MUTATION_SIZE = "cassandra.batchmutate.size";
    public static final String CASSANDRA_SLICE_PREDICATE_COLUMN_NAMES = "cassandra.slice.predicate.column_names";
    public static final String CASSANDRA_SLICE_PREDICATE_RANGE_START = "cassandra.slice.predicate.range.start";
    public static final String CASSANDRA_SLICE_PREDICATE_RANGE_FINISH = "cassandra.slice.predicate.range.finish";
    public static final String CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR = "cassandra.slice.predicate.range.comparator";
    public static final String CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED = "cassandra.slice.predicate.range.reversed";
    public static final String CASSANDRA_SLICE_PREDICATE_RANGE_COUNT = "cassandra.slice.predicate.range.count";

    public static final String COLUMN_FAMILY_COMMENT = "comment";
    public static final String READ_REPAIR_CHANCE = "read_repair_chance";
    public static final String DCLOCAL_READ_REPAIR_CHANCE = "dclocal_read_repair_chance";
    public static final String GC_GRACE_SECONDS = "gc_grace_seconds";
    public static final String BLOOM_FILTER_FP_CHANCE = "bloom_filter_fp_chance";
    public static final String COMPACTION = "compaction";
    public static final String COMPRESSION = "compression";
    public static final String REPLICATE_ON_WRITE = "replicate_on_write";
    public static final String CACHING = "caching";

    public static final String CASSANDRA_CONSISTENCY_LEVEL = "cassandra.consistency.level";
    public static final String CASSANDRA_THRIFT_MODE = "cassandra.thrift.mode";

    public static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
    public static final int DEFAULT_RANGE_BATCH_SIZE = 1000;
    public static final int DEFAULT_SLICE_PREDICATE_SIZE = 1000;
    public static final String DEFAULT_CASSANDRA_HOST = "localhost";
    public static final String DEFAULT_CASSANDRA_PORT = "9160";
    public static final String DEFAULT_CONSISTENCY_LEVEL = "ONE";
    public static final int DEFAULT_BATCH_MUTATION_SIZE = 500;
    public static final String DELIMITER = ",";

    /* names of columns from SerdeParameters */
    protected List<String> cassandraColumnNames;

    protected TableMapping mapping;

    protected ObjectInspector cachedObjectInspector;
    protected LazySimpleSerDe.SerDeParameters serdeParams;
    protected String cassandraKeyspace;
    protected String cassandraColumnFamily;
    protected List<Text> cassandraColumnNamesText;

    protected abstract void initCassandraSerDeParameters(Configuration job, Properties tbl, String serdeName)
            throws SerDeException;

    /**
     * Create the object inspector.
     *
     * @return object inspector
     */
    public abstract ObjectInspector createObjectInspector();

    /*
   * Turns obj (a Hive Row) into a cassandra data format.
   */
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + objInspector.getTypeName());
        }
        // Prepare the field ObjectInspectors
        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);
        List<? extends StructField> declaredFields =
                (serdeParams.getRowTypeInfo() != null &&
                        ((StructTypeInfo) serdeParams.getRowTypeInfo())
                                .getAllStructFieldNames().size() > 0) ?
                        ((StructObjectInspector) getObjectInspector()).getAllStructFieldRefs()
                        : null;
        try {
            return mapping.getWritable(fields, list, declaredFields);
        } catch (IOException e) {
            throw new SerDeException("Unable to serialize this object! " + e);
        }
    }

   /**
   * @see org.apache.hadoop.hive.serde2.Deserializer#deserialize(org.apache.hadoop.io.Writable)
   * Turns a Cassandra row into a Hive row.
   */
    public abstract Object deserialize(Writable w) throws SerDeException;

    /**
     * Parse cassandra keyspace from table properties.
     *
     * @param tbl table properties
     * @return cassandra keyspace
     * @throws org.apache.hadoop.hive.serde2.SerDeException
     *          error parsing keyspace
     */
    protected String parseCassandraKeyspace(Properties tbl) throws SerDeException {
        String result = tbl.getProperty(CASSANDRA_KEYSPACE_NAME);

        if (result == null) {

            result = tbl
                    .getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME);

            if (result == null) {
                throw new SerDeException("CassandraKeyspace not defined" + tbl.toString());
            }

            if (result.indexOf(".") != -1) {
                result = result.substring(0, result.indexOf("."));
            }
        }

        return result;
    }

    /**
     * Parse cassandra column family name from table properties.
     *
     * @param tbl table properties
     * @return cassandra column family name
     * @throws org.apache.hadoop.hive.serde2.SerDeException
     *          error parsing column family name
     */
    protected String parseCassandraColumnFamily(Properties tbl) throws SerDeException {
        String result = tbl.getProperty(CASSANDRA_CF_NAME);

        if (result == null) {

            result = tbl
                    .getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME);

            if (result == null) {
                throw new SerDeException("CassandraColumnFamily not defined" + tbl.toString());
            }

            if (result.indexOf(".") != -1) {
                result = result.substring(result.indexOf(".") + 1);
            }
        }

        return result;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return cachedObjectInspector;
    }

    /**
     * Set the table mapping. We only support transposed mapping and regular table mapping for now.
     *
     * @throws org.apache.hadoop.hive.serde2.SerDeException
     *
     */
    protected abstract void setTableMapping() throws SerDeException;

    /**
     * Trim the white spaces, new lines from the input array.
     *
     * @param input a input string array
     * @return a trimmed string array
     */
    protected static String[] trim(String[] input) {
        String[] trimmed = new String[input.length];
        for (int i = 0; i < input.length; i++) {
            trimmed[i] = input[i].trim();
        }

        return trimmed;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    /**
     * @return the name of the cassandra keyspace as parsed from table properties
     */
    public String getCassandraKeyspace(){
        return cassandraKeyspace;
    }

    /**
     * @return the name of the cassandra columnfamily as parsed from table properties
     */
    public String getCassandraColumnFamily(){
        return cassandraColumnFamily;
    }
}
