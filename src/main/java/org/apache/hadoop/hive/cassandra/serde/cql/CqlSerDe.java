package org.apache.hadoop.hive.cassandra.serde.cql;

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
import org.apache.hadoop.hive.cassandra.input.cql.LazyCqlRow;
import org.apache.hadoop.hive.cassandra.output.cql.CqlPut;
import org.apache.hadoop.hive.cassandra.serde.CassandraLazyFactory;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlSerDe extends AbstractCassandraSerDe {

    public static final Logger LOG = LoggerFactory.getLogger(CqlSerDe.class);

    public static final String CASSANDRA_COLUMN_FAMILY_PRIMARY_KEY = "cql.primarykey";

    protected LazyCqlRow lazyCqlRow;
    protected List<Text> cassandraColumnNamesText;

  public static final String CASSANDRA_VALIDATOR_TYPE = "cassandra.cf.validatorType"; // validator type

  public static final AbstractType DEFAULT_VALIDATOR_TYPE = BytesType.instance;

  private List<AbstractType> validatorType;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        initCassandraSerDeParameters(conf, tbl, getClass().getName());
        cachedObjectInspector = createObjectInspector();

        lazyCqlRow = new LazyCqlRow(
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
            throw new SerDeException(getClass().getName() + ": expects MapWritable not " + w.getClass().getName());
        }

        MapWritable columnMap = (MapWritable) w;
        lazyCqlRow.init(columnMap, cassandraColumnNames, cassandraColumnNamesText);
        LOG.debug("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        LOG.debug(lazyCqlRow.getFieldsAsList().toString());
        return lazyCqlRow;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return CqlPut.class;
    }

  /**
   * Initialize the cassandra serialization and deserialization parameters from table properties and configuration.
   *
   * @param job
   * @param tbl
   * @param serdeName
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   *
   */
  @Override
  protected void initCassandraSerDeParameters(Configuration job, Properties tbl, String serdeName)
          throws SerDeException {
    cassandraKeyspace = parseCassandraKeyspace(tbl);
    cassandraColumnFamily = parseCassandraColumnFamily(tbl);
    cassandraColumnNames = parseOrCreateColumnMapping(tbl);

    cassandraColumnNamesText = new ArrayList<Text>();
    for (String columnName : cassandraColumnNames) {
      cassandraColumnNamesText.add(new Text(columnName));
    }

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
      ObjectInspector ob = CassandraLazyFactory.createLazyStructInspector(
            serdeParams.getColumnNames(),
            serdeParams.getColumnTypes(),
            validatorType,
            serdeParams.getSeparators(),
            serdeParams.getNullSequence(),
            serdeParams.isLastColumnTakesRest(),
            serdeParams .isEscaped(),
            serdeParams.getEscapeChar());
    return ob;
  }

  /**
   * Parse or create the validator types. If <code>CASSANDRA_VALIDATOR_TYPE</code> is defined in the property,
   * it will be used for parsing; Otherwise an empty list will be returned;
   *
   * @param tbl property list
   * @return a list of validator type or an empty list if no property is defined
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   *          when the number of validator types is fewer than the number of columns or when no matching
   *          validator type is found in Cassandra.
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
     * Parses the cassandra columns mapping to identify the column name.
     * One of the Hive table columns maps to the cassandra row key, by default the
     * first column.
     *
     * @param columnMapping - the column mapping specification to be parsed
     * @return a list of cassandra column names
     */
    public static List<String> parseColumnMapping(String columnMapping) {
        assert StringUtils.isNotBlank(columnMapping);
        String[] columnArray = columnMapping.split(",");
        String[] trimmedColumnArray = trim(columnArray);

        return Arrays.asList(trimmedColumnArray);
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
        if (StringUtils.isBlank(tblColumnStr)) {
            throw new IllegalArgumentException("table must have columns");
        }

        //String[] colNames = tblColumnStr.split(",");

        //return createColumnMappingString(colNames);
        return tblColumnStr;
    }

    /**
     * Parse the column mappping from table properties. If cassandra.columns.mapping
     * is defined in the property, use it to create the mapping. Otherwise, create the mapping from table
     * columns using the default mapping.
     *
     * @param tbl table properties
     * @return A list of column names
     * @throws org.apache.hadoop.hive.serde2.SerDeException
     *
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
     * Set the table mapping. We only support transposed mapping and regular table mapping for now.
     *
     * @throws org.apache.hadoop.hive.serde2.SerDeException
     *
     */
    protected void setTableMapping() throws SerDeException {
        mapping = new CqlRegularTableMapping(cassandraColumnFamily, cassandraColumnNames, serdeParams);
    }
    
    

}
