package org.apache.hadoop.hive.cassandra.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.LazyCassandraRow;
import org.apache.hadoop.hive.cassandra.output.CassandraPut;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraColumnSerDe extends AbstractCassandraSerDe {

    public static final Logger LOG = LoggerFactory.getLogger(CassandraColumnSerDe.class);

    public static final String CASSANDRA_VALIDATOR_TYPE = "cassandra.cf.validatorType"; // validator type

    public static final AbstractType DEFAULT_VALIDATOR_TYPE = BytesType.instance;

    private List<AbstractType> validatorType;

    public static final String CASSANDRA_ENABLE_WIDEROW_ITERATOR = "cassandra.enable.widerow.iterator";

    public static final String CASSANDRA_SPECIAL_COLUMN_KEY = "row_key";
    public static final String CASSANDRA_SPECIAL_COLUMN_COL = "column_name";
    public static final String CASSANDRA_SPECIAL_COLUMN_SCOL= "sub_column_name";
    public static final String CASSANDRA_SPECIAL_COLUMN_VAL = "value";

    public static final String CASSANDRA_KEY_COLUMN       = ":key";
    public static final String CASSANDRA_COLUMN_COLUMN    = ":column";
    public static final String CASSANDRA_SUBCOLUMN_COLUMN = ":subcolumn";
    public static final String CASSANDRA_VALUE_COLUMN     = ":value";

    /* names of columns from SerdeParameters */
    protected List<String> cassandraColumnNames;
    /* index of key column in results */
    protected int iKey;
    protected LazyCassandraRow cachedCassandraRow;
    protected List<BytesWritable> cassandraColumnNamesBytes;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        initCassandraSerDeParameters(conf, tbl, getClass().getName());
        cachedObjectInspector = createObjectInspector();

        cachedCassandraRow = new LazyCassandraRow(
                (LazySimpleStructObjectInspector) cachedObjectInspector);

        if (LOG.isDebugEnabled()) {
            LOG.debug("AbstractCassandraSerDe initialized with : columnNames = "
                    + StringUtils.join(serdeParams.getColumnNames(), ",")
                    + " columnTypes = "
                    + StringUtils.join(serdeParams.getColumnTypes(), ",")
                    + " cassandraColumnMapping = "
                    + cassandraColumnNames);
        }
    }

    /*
     *
     * @see org.apache.hadoop.hive.serde2.Deserializer#deserialize(org.apache.hadoop.io.Writable)
     * Turns a Cassandra row into a Hive row.
     */
    @Override
    public Object deserialize(Writable w) throws SerDeException {
        if (!(w instanceof MapWritable)) {
            throw new SerDeException(getClass().getName() + ": expects MapWritable not "+w.getClass().getName());
        }

        MapWritable columnMap = (MapWritable) w;
        cachedCassandraRow.init(columnMap, cassandraColumnNames, cassandraColumnNamesBytes);
        return cachedCassandraRow;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return CassandraPut.class;
    }

    /**
     * Initialize the cassandra serialization and deserialization parameters from table properties and configuration.
     *
     * @param job
     * @param tbl
     * @param serdeName
     * @throws SerDeException
     */
    @Override
    protected void initCassandraSerDeParameters(Configuration job, Properties tbl, String serdeName)
            throws SerDeException {
        cassandraKeyspace = parseCassandraKeyspace(tbl);
        cassandraColumnFamily = parseCassandraColumnFamily(tbl);
        cassandraColumnNames = parseOrCreateColumnMapping(tbl);

        cassandraColumnNamesBytes = new ArrayList<BytesWritable>();
        for (String columnName : cassandraColumnNames) {
            cassandraColumnNamesBytes.add(new BytesWritable(columnName.getBytes()));
        }

        iKey = cassandraColumnNames.indexOf(CassandraColumnSerDe.CASSANDRA_KEY_COLUMN);

        serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

        validatorType = parseOrCreateValidatorType(tbl);

        setTableMapping();

        if (cassandraColumnNames.size() != serdeParams.getColumnNames().size()) {
            throw new SerDeException(serdeName + ": columns has " +
                    serdeParams.getColumnNames().size() +
                    " elements while cassandra.columns.mapping has " +
                    cassandraColumnNames.size() + " elements" +
                    " (counting the key if implicit)");
        }

        // we just can make sure that "StandardColumn:" is mapped to MAP<String,?>
        for (int i = 0; i < cassandraColumnNames.size(); i++) {
            String cassandraColName = cassandraColumnNames.get(i);
            if (cassandraColName.endsWith(":")) {
                TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
                if ((typeInfo.getCategory() != Category.MAP) ||
                        (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getTypeName()
                                != Constants.STRING_TYPE_NAME)) {

                    throw new SerDeException(
                            serdeName + ": Cassandra column family '"
                                    + cassandraColName
                                    + "' should be mapped to map<string,?> but is mapped to "
                                    + typeInfo.getTypeName());
                }
            }
        }
    }

    @Override
    public ObjectInspector createObjectInspector() {
        return CassandraLazyFactory.createLazyStructInspector(
                serdeParams.getColumnNames(),
                serdeParams.getColumnTypes(),
                validatorType,
                serdeParams.getSeparators(),
                serdeParams.getNullSequence(),
                serdeParams.isLastColumnTakesRest(),
                serdeParams.isEscaped(),
                serdeParams.getEscapeChar());
    }

    /**
     * Parse or create the validator types. If <code>CASSANDRA_VALIDATOR_TYPE</code> is defined in the property,
     * it will be used for parsing; Otherwise an empty list will be returned;
     *
     * @param tbl property list
     * @return a list of validator type or an empty list if no property is defined
     * @throws SerDeException when the number of validator types is fewer than the number of columns or when no matching
     *                        validator type is found in Cassandra.
     */
    private List<AbstractType> parseOrCreateValidatorType(Properties tbl)
            throws SerDeException {
        String prop = tbl.getProperty(CASSANDRA_VALIDATOR_TYPE);
        List<AbstractType> result = new ArrayList<AbstractType>();

        if (prop != null) {
            assert StringUtils.isNotBlank(prop);
            String[] validators = prop.split(",");
            String[] trimmedValidators = trim(validators);

            List<String> columnList = Arrays.asList(trimmedValidators);
            result = parseValidatorType(columnList);

            if (result.size() < cassandraColumnNames.size()) {
                throw new SerDeException("There are fewer validator types defined than the column names. " +
                        "ColumnaName size: " + cassandraColumnNames.size() + " ValidatorType size: " + result.size());
            }
        }

        return result;
    }

    /**
     * Parses the cassandra columns mapping to identify the column name.
     * One of the Hive table columns maps to the cassandra row key, by default the
     * first column.
     *
     * @param columnList a list of column validator type in String format
     * @return a list of cassandra validator type
     */
    private List<AbstractType> parseValidatorType(List<String> columnList)
            throws SerDeException {
        List<AbstractType> types = new ArrayList<AbstractType>();

        for (String str : columnList) {
            if (StringUtils.isBlank(str)) {
                types.add(DEFAULT_VALIDATOR_TYPE);
            } else {
                try {
                    types.add(TypeParser.parse(str));
                } catch (ConfigurationException e) {
                    throw new SerDeException("Invalid Cassandra validator type ' " + str + "'");
                } catch (SyntaxException e) {
                    throw new SerDeException(e);
                }
            }
        }

        return types;
    }

    /**
     * Set the table mapping. We only support transposed mapping and regular table mapping for now.
     *
     * @throws SerDeException
     */
    protected void setTableMapping() throws SerDeException {
        if (isTransposed(cassandraColumnNames)) {
            mapping = new TransposedMapping(cassandraColumnFamily, cassandraColumnNames, serdeParams);
        } else {
            mapping = new RegularTableMapping(cassandraColumnFamily, cassandraColumnNames, serdeParams);
        }
    }

    /**
     * Parses the cassandra columns mapping to identify the column name.
     * One of the Hive table columns maps to the cassandra row key, by default the
     * first column.
     *
     * @param columnMapping - the column mapping specification to be parsed
     * @return a list of cassandra column names
     */
    public static List<String> parseColumnMapping(String columnMapping)
    {
        assert StringUtils.isNotBlank(columnMapping);
        String[] columnArray = columnMapping.split(",");
        String[] trimmedColumnArray = trim(columnArray);

        List<String> columnList = Arrays.asList(trimmedColumnArray);

        int iKey = columnList.indexOf(CASSANDRA_KEY_COLUMN);

        if (iKey == -1) {
            columnList = new ArrayList<String>(columnList);
            columnList.add(0, CASSANDRA_KEY_COLUMN);
        }

        return columnList;
    }

    /**
     * Return the column mapping created from column names.
     *
     * @param colNames column names in array format
     * @return column mapping string
     */
    public static String createColumnMappingString(String[] colNames) {

        //First check of this is a "transposed_table" by seeing if all
        //values match our special column names
        boolean isTransposedTable = true;
        boolean hasKey = false;
        boolean hasVal = false;
        boolean hasCol = false;
        boolean hasSubCol = false;
        String transposedMapping = "";
        for(String column : colNames) {
            if (column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_KEY)){
                transposedMapping += ","+CASSANDRA_KEY_COLUMN;
                hasKey = true;
            } else if(column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_COL)){
                transposedMapping += ","+CASSANDRA_COLUMN_COLUMN;
                hasCol = true;
            } else if(column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_SCOL)){
                transposedMapping += ","+CASSANDRA_SUBCOLUMN_COLUMN;
                hasSubCol = true;
            } else if(column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_VAL)){
                transposedMapping += ","+CASSANDRA_VALUE_COLUMN;
                hasVal = true;
            } else {
                isTransposedTable = false;
                break;
            }
        }

        if(isTransposedTable && !(colNames.length == 1 && hasKey)){

            if(!hasKey || !hasVal || !hasCol ) {
                throw new IllegalArgumentException("Transposed table definition missing required fields!");
            }

            return transposedMapping.substring(1);//skip leading ,
        }

        //Regular non-transposed logic. The first column maps to the key automatically.
        StringBuilder mappingStr = new StringBuilder(CASSANDRA_KEY_COLUMN);
        for (int i = 1; i < colNames.length; i++) {
            mappingStr.append(",");
            mappingStr.append(colNames[i]);
        }

        return mappingStr.toString();
    }

    /*
     * Creates the cassandra column mappings from the hive column names.
     * This would be triggered when no cassandra.columns.mapping has been defined
     * in the user query.
     *
     * row_key is a special column name, it maps to the key of a row in cassandra;
     * column_name maps to the name of a column/supercolumn;
     * value maps to the value of a column;
     * sub_column_name maps to the name of a column (This can only be used for a super column family.)
     *
     * @param tblColumnStr hive table column names
     */
    public static String createColumnMappingString(String tblColumnStr) {
        if(StringUtils.isBlank(tblColumnStr)) {
            throw new IllegalArgumentException("table must have columns");
        }

        String[] colNames = tblColumnStr.split(",");

        return createColumnMappingString(colNames);
    }

    /**
     * Parse the column mappping from table properties. If cassandra.columns.mapping
     * is defined in the property, use it to create the mapping. Otherwise, create the mapping from table
     * columns using the default mapping.
     *
     * @param tbl table properties
     * @return A list of column names
     * @throws SerDeException
     */
    protected List<String> parseOrCreateColumnMapping(Properties tbl) throws SerDeException {
        String prop = tbl.getProperty(AbstractCassandraSerDe.CASSANDRA_COL_MAPPING);

        if (prop != null) {
            return parseColumnMapping(prop);
        } else {
            String tblColumnStr = tbl.getProperty(Constants.LIST_COLUMNS);

            if (tblColumnStr != null) {
                //auto-create
                String mappingStr = createColumnMappingString(tblColumnStr);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("table column string: " + tblColumnStr);
                    LOG.debug("Auto-created mapping string: " + mappingStr);
                }

                return Arrays.asList(mappingStr.split(","));

            } else {
                throw new SerDeException("Can't find table column definitions");
            }
        }
    }

    /**
     * Return if a table is a transposed. A table is transposed when the column mapping is like
     * (:key, :column, :value) or (:key, :column, :subcolumn, :value).
     *
     * @return true if a table is transposed, otherwise false
     */
    public static boolean isTransposed(List<String> columnNames)
    {
        if(columnNames == null || columnNames.size() == 0) {
            throw new IllegalArgumentException("no cassandra column information found");
        }

        boolean hasKey = false;
        boolean hasColumn = false;
        boolean hasValue = false;
        boolean hasSubColumn = false;

        for (String column : columnNames) {
            if (column.equalsIgnoreCase(CASSANDRA_KEY_COLUMN)) {
                hasKey = true;
            } else if (column.equalsIgnoreCase(CASSANDRA_COLUMN_COLUMN)) {
                hasColumn = true;
            } else if (column.equalsIgnoreCase(CASSANDRA_SUBCOLUMN_COLUMN)) {
                hasSubColumn = true;
            } else if (column.equalsIgnoreCase(CASSANDRA_VALUE_COLUMN)) {
                hasValue = true;
            } else {
                return false;
            }
        }

        //only requested row key
        if(columnNames.size() == 1 && hasKey) {
            return false;
        }

        if(!hasKey || !hasValue || !hasColumn) {
            return false;
        }

        return true;
    }

}
