package org.apache.hadoop.hive.cassandra.serde.cql;

import org.apache.hadoop.hive.cassandra.output.cql.CqlColumn;
import org.apache.hadoop.hive.cassandra.output.cql.CqlPut;
import org.apache.hadoop.hive.cassandra.serde.TableMapping;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CqlRegularTableMapping extends TableMapping {

    public CqlRegularTableMapping(
            String colFamily,
            List<String> columnNames,
            SerDeParameters serdeParams) {
        super(colFamily, columnNames, serdeParams);
    }

    @Override
    public Writable getWritable(List<? extends StructField> fields,
                                List<Object> list,
                                List<? extends StructField> declaredFields) throws IOException {
        return write(fields, list, declaredFields);
    }

    public Writable write(
            List<? extends StructField> fields,
            List<Object> list,
            List<? extends StructField> declaredFields) throws IOException {
        CqlPut cqlPut = new CqlPut();

        for (int i = 0; i < cassandraColumnNames.size(); i++) {
            String cassandraColumn = cassandraColumnNames.get(i);

            // Get the field objectInspector and the field object.
            ObjectInspector foi = fields.get(i).getFieldObjectInspector();
            ObjectInspector doi = declaredFields.get(i).getFieldObjectInspector();

            Object f = null;
            if (list == null) {
                return null;
            } else {
                assert i < list.size();
                f = list.get(i);
            }

            // If the field corresponds to a column family in cassandra
            // (when would someone ever need this?)
            if (cassandraColumn.endsWith(":")) {
                MapObjectInspector moi = (MapObjectInspector) foi;
                ObjectInspector koi = moi.getMapKeyObjectInspector();
                ObjectInspector voi = moi.getMapValueObjectInspector();

                Map<?, ?> map = moi.getMap(f);
                if (map == null) {
                    return null;
                } else {
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        // Get the Key
                        serializeStream.reset();
                        serialize(entry.getKey(), koi, koi, 3);

                        // Get the column-qualifier
                        byte[] columnQualifier = new byte[serializeStream.getCount()];
                        System.arraycopy(serializeStream.getData(), 0, columnQualifier, 0, serializeStream.getCount());

                        // Get the Value
                        serializeStream.reset();

                        boolean isNotNull = serialize(entry.getValue(), voi, voi, 3);
                        if (!isNotNull) {
                            continue;
                        }
                        byte[] value = new byte[serializeStream.getCount()];
                        System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());

                        CqlColumn cqlColumn = new CqlColumn();
                        cqlColumn.setColumn(cassandraColumn.getBytes());
                        cqlColumn.setValue(value);
                        cqlColumn.setColumnFamily(cassandraColumnFamily);
                        cqlColumn.setTimeStamp(System.currentTimeMillis());
                        cqlPut.getColumns().add(cqlColumn);
                    }
                }
            } else {
                CqlColumn cqlColumn = new CqlColumn();
                cqlColumn.setColumn(cassandraColumn.getBytes());
                byte[] value = serializeToBytes(foi, doi, f, useJsonSerialize(i, declaredFields));
                cqlColumn.setValue(value);
                cqlColumn.setTimeStamp(System.currentTimeMillis());
                cqlPut.getColumns().add(cqlColumn);
            }
        }
        return cqlPut;
    }
}
