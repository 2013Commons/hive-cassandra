package org.apache.hadoop.hive.cassandra.cql;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.hadoop.hive.cassandra.CassandraClientHolder;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyCassandraUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.thrift.protocol.TBinaryProtocol;

public class CqlPushdownPredicate {

  private static final Logger logger = LoggerFactory.getLogger(CqlPushdownPredicate.class);

  /**
   * Get metadata for the columns which have secondary indexes
   *
   * @param host
   * @param port
   * @param ksName keyspace name
   * @param cfName column family name
   * @return A set of ColumnDefs representing the indexed columns of the cf.
   *         Only the name of the column and its validation class are required,
   *         at the moment, so all other fields are left unset
   * @throws CassandraException if a problem is encountered communicating with Cassandra
   */
  public static Set<ColumnDef> getIndexedColumns(String host, int port, String ksName, String cfName) throws CassandraException {
    String getIdxColTmp = "select column_name, index_name, validator from system.schema_columns " +
            "where keyspace_name='%s' and columnfamily_name = '%s';";
    String getIdxColQuery = String.format(getIdxColTmp, ksName, cfName);

    Set<ColumnDef> indexedColumns = new HashSet<ColumnDef>();

    final CassandraClientHolder client = new CassandraProxyClient(host, port, true, true).getClientHolder();
    try {
      CqlResult result = client.getClient().execute_cql3_query(ByteBufferUtil.bytes(getIdxColQuery),
              Compression.NONE, ConsistencyLevel.ONE);

      List<CqlRow> cfColumns = result.getRows();

      for (CqlRow cfColumn : cfColumns) {
          //each row(cfColumn) in the returned result has index_name and validator at index 1,2.
        if (cfColumn.columns.get(1).isSetValue() && cfColumn.columns.get(2).isSetValue()) {
          ColumnDef cd = new ColumnDef();
          cd.setName(cfColumn.columns.get(1).value);
          cd.setValidation_class(ByteBufferUtil.string(cfColumn.columns.get(2).value));
          indexedColumns.add(cd);
        }
      }
    } catch (InvalidRequestException e) {
      throw new CassandraException(e);
    } catch (UnavailableException e) {
      throw new CassandraException(e);
    } catch (TimedOutException e) {
      throw new CassandraException(e);
    } catch (SchemaDisagreementException e) {
      throw new CassandraException(e);
    } catch (TException e) {
      throw new CassandraException(e);
    } catch (CharacterCodingException e) {
      throw new CassandraException(e);
    }
    return indexedColumns;
  }

  /**
   * Serialize a set of ColumnDefs for indexed columns, so that
   * it can be written to Job configuration
   *
   * @param columns column metadata
   * @return serialized form
   */
  public static String serializeIndexedColumns(Set<ColumnDef> columns) {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      List<String> hexStrings = new ArrayList<String>();
      for (ColumnDef column : columns) {
        String encoded = Hex.bytesToHex(serializer.serialize(column));
        logger.info("Encoded column def: " + encoded);
        hexStrings.add(encoded);
      }
      return Joiner.on(AbstractCassandraSerDe.DELIMITER).join(hexStrings);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serialize a set of ColumnDefs for indexed columns, read
   * from Job configuration
   *
   * @param serialized column metadata
   * @return list of column metadata objects which may be empty, but not null
   */
  public static Set<ColumnDef> deserializeIndexedColumns(String serialized) {
    Set<ColumnDef> columns = new HashSet<ColumnDef>();
    if (null == serialized) {
      return columns;
    }

    Iterable<String> strings = Splitter.on(AbstractCassandraSerDe.DELIMITER).omitEmptyStrings().trimResults().split(serialized);
    TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    for (String encoded : strings) {
      ColumnDef column = new ColumnDef();
      try {
        logger.info("Encoded column def: " + encoded);
        deserializer.deserialize(column, Hex.hexToBytes(encoded));
      } catch (TException e) {
        logger.warn("Error deserializing indexed column definition", e);
      }
      if (null == column.getName() || null == column.validation_class) {
        continue;
      }
      columns.add(column);
    }
    return columns;
  }

  /**
   * Given a set of indexed column names, return an IndexPredicateAnalyzer
   *
   * @param indexedColumns names of indexed columns
   * @return IndexPredicateAnalyzer
   */
  public static IndexPredicateAnalyzer newIndexPredicateAnalyzer(Set<ColumnDef> indexedColumns) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // we only support C*'s set of comparisons = > >= =< <
    analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());

    for (ColumnDef column : indexedColumns) {
      analyzer.allowColumnName(new String(column.getName()));
    }

    return analyzer;
  }

  /**
   * An IndexClause in C* must always include at least 1 EQ condition.
   * Validate this constraint is satisified by the list of IndexSearchConditions
   *
   * @return true if there is an EQ operator present, otherwise false
   */
  public static boolean verifySearchConditions(List<IndexSearchCondition> conditions) {
    for (IndexSearchCondition thisCon : conditions) {
      if (thisCon.getComparisonOp().equals(GenericUDFOPEqual.class.getName())) {
        return true;
      }
    }

    return false;
  }

  /**
   * Translate the list of Hive SearchConditions into C* IndexExpressions.
   *
   * @param conditions a list of index search condition
   * @return list of IndexExpressions, which may be empty but not null
   */
  public static List<IndexExpression> translateSearchConditions(List<IndexSearchCondition> conditions, Set<ColumnDef> indexedColumns) throws IOException {
    List<IndexExpression> exps = new ArrayList<IndexExpression>();
    for (IndexSearchCondition thisCond : conditions) {
      exps.add(translateSearchCondition(thisCond, indexedColumns));
    }
    return exps;
  }

  private static IndexExpression translateSearchCondition(IndexSearchCondition condition, Set<ColumnDef> columnInfos) throws IOException {
    IndexExpression expr = new IndexExpression();
    String columnName = condition.getColumnDesc().getColumn();
    expr.setColumn_name(columnName.getBytes());
    expr.setOp(getIndexOperator(condition.getComparisonOp()));

    ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(condition.getConstantDesc());
    byte[] value;
    try {
      ObjectInspector objInspector = eval.initialize(null);
      Object writable = eval.evaluate(null);
      ByteStream.Output serializeStream = new ByteStream.Output();

      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) objInspector;
      AbstractType validator = getValidator(columnInfos, columnName);
      ByteBuffer bytes = getIndexExpressionValue(condition.getConstantDesc(), poi, writable, validator);
      serializeStream.write(ByteBufferUtil.getArray(bytes));

      value = new byte[serializeStream.getCount()];
      System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());
    } catch (HiveException e) {
      throw new IOException(e);
    }
    expr.setValue(value);
    logger.info("IndexExpression.value : {}", new String(expr.getValue()));
    return expr;
  }

  private static AbstractType getValidator(Set<ColumnDef> columnInfos, String columnName) {
    for (ColumnDef column : columnInfos) {
      if (new String(column.getName()).equals(columnName)) {
        try {
          return TypeParser.parse(column.validation_class);
        } catch (ConfigurationException e) {
          logger.error("Error creating validator from string {}", column.validation_class);
          throw new RuntimeException(e);
        } catch (SyntaxException e) {
          logger.error("Syntax exception in parsing: \n {}", e.getMessage());
          throw new RuntimeException(e);
        }
      }
    }
    logger.error("Error finding validator class for column {}", columnName);
    throw new RuntimeException("Error finding validator class for column " + columnName);
  }

  private static ByteBuffer getIndexExpressionValue(ExprNodeConstantDesc constantDesc, PrimitiveObjectInspector poi, Object writable, AbstractType validator) {
    logger.info("Primitive Category: {}, Validation class: {}, CassandraType: {}",
            new Object[]{poi.getPrimitiveCategory(), validator.getClass().getName(), LazyCassandraUtils.getCassandraType(poi)});
    switch (poi.getPrimitiveCategory()) {
      case TIMESTAMP:
        String dateString = new java.sql.Date(
                ((java.sql.Timestamp) poi.getPrimitiveJavaObject(writable)).getTime())
                .toString();
        return validator.fromString(dateString);
      case BINARY:
        byte[] bytes = ((ByteArrayRef) poi.getPrimitiveJavaObject(writable)).getData();

        // this will only work if the value has been cast using one of the UDFs
        // UDFHexToBytes, UDFUuid, UDFDecimal, UDFVarint
        return ByteBuffer.wrap(bytes);
      default:
        return validator.fromString(constantDesc.getValue().toString());
    }
  }

  private static IndexOperator getIndexOperator(String str) throws IOException {
    if (str.equals(GenericUDFOPEqual.class.getName())) {
      return IndexOperator.EQ;
    } else if (str.equals(GenericUDFOPEqualOrGreaterThan.class.getName())) {
      return IndexOperator.GTE;
    } else if (str.equals(GenericUDFOPGreaterThan.class.getName())) {
      return IndexOperator.GT;
    } else if (str.equals(GenericUDFOPEqualOrLessThan.class.getName())) {
      return IndexOperator.LTE;
    } else if (str.equals(GenericUDFOPLessThan.class.getName())) {
      return IndexOperator.LT;
    } else {
      throw new IOException("Unable to get index operator matches " + str);
    }
  }

}
