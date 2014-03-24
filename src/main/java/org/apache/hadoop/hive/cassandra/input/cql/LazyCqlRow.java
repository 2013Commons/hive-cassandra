package org.apache.hadoop.hive.cassandra.input.cql;

import org.apache.hadoop.hive.cassandra.serde.CassandraLazyFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.mortbay.log.Log;

public class LazyCqlRow extends LazyStruct {
  static final Logger LOG = LoggerFactory.getLogger(LazyCqlRow.class);

  private List<String> cassandraColumns;
  private List<Text> cassandraColumnsBB;
  private MapWritable columnMap;
  private ArrayList<Object> cachedList;

  public LazyCqlRow(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  public void init(MapWritable columnMap, List<String> cassandraColumns, List<Text> cassandraColumnsBB) {
    this.columnMap = columnMap;
    this.cassandraColumns = cassandraColumns;
    this.cassandraColumnsBB = cassandraColumnsBB;
    // these should always be equal
    if (cassandraColumns.size() != cassandraColumnsBB.size()) {
      throw new IllegalStateException();
    }

    setParsed(false);
  }

  private void parse() {
    if (getFields() == null) {
      List<? extends StructField> fieldRefs = ((StructObjectInspector) getInspector())
              .getAllStructFieldRefs();
      setFields(new LazyObject[fieldRefs.size()]);
      for (int i = 0; i < getFields().length; i++) {
        String cassandraColumn = this.cassandraColumns.get(i);
        if (cassandraColumn.endsWith(":")) {
          // want all columns as a map
          getFields()[i] = new LazyCqlCellMap((LazyMapObjectInspector)
                  fieldRefs.get(i).getFieldObjectInspector());
        } else {
          // otherwise only interested in a single column

          getFields()[i] = CassandraLazyFactory.createLazyObject(
                  fieldRefs.get(i).getFieldObjectInspector());
        }
      }
      setFieldInited(new boolean[getFields().length]);
    }
    Arrays.fill(getFieldInited(), false);
    setParsed(true);
  }

  @Override
  public Object getField(int fieldID) {
    if (!getParsed()) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }

  private Object uncheckedGetField(int fieldID) {
      // Error
      
      
      
    if (!getFieldInited()[fieldID]) {
      getFieldInited()[fieldID] = true;
      ByteArrayRef ref = null;
      String columnName = cassandraColumns.get(fieldID);
      Text columnNameText = cassandraColumnsBB.get(fieldID);

      LazyObject obj = getFields()[fieldID];
      if (columnName.endsWith(":")) {
        // user wants all columns as a map
        // TODO this into a LazyCassandraCellMap
        return null;
      } else {
        // user wants the value of a single column
        BytesWritable columnValue = (BytesWritable) columnMap.get(columnNameText);
        
       // Log.debug("value correct?+ : columnName" + new String(columnValue.copyBytes()));
        
        if (columnValue != null) {
          ref = new ByteArrayRef();
          ref.setData(columnValue.getBytes());
        } else {
          return null;
        }
      }
      if (ref != null) {
        obj.init(ref, 0, ref.getData().length);
      }
    }
    
    //Log.debug("value correct?+ : columnName" + getFields()[fieldID].getObject().toString());
    
    return getFields()[fieldID].getObject();
  }

  /**
   * Get the values of the fields as an ArrayList.
   *
   * @return The values of the fields as an ArrayList.
   */
  @Override
  public ArrayList<Object> getFieldsAsList() {
    if (!getParsed()) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }
}
