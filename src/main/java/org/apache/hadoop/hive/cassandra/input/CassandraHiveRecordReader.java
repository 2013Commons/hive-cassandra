package org.apache.hadoop.hive.cassandra.input;

import org.apache.cassandra.hadoop2.ColumnFamilyRecordReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.serde.CassandraColumnSerDe;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import org.apache.cassandra.db.Column;

public class CassandraHiveRecordReader extends RecordReader<BytesWritable, MapWritable>
  implements org.apache.hadoop.mapred.RecordReader<BytesWritable, MapWritable> {
  static final Logger LOG = LoggerFactory.getLogger(CassandraHiveRecordReader.class);

  private final boolean isTransposed;
  private final ColumnFamilyRecordReader cfrr;
  private Iterator<Map.Entry<ByteBuffer, Column>> columnIterator = null;
  private Map.Entry<ByteBuffer, Column> currentEntry;
  private Iterator<Column> subColumnIterator = null;
  private BytesWritable currentKey = null;
  private final MapWritable currentValue = new MapWritable();

  public static final BytesWritable keyColumn = new BytesWritable(CassandraColumnSerDe.CASSANDRA_KEY_COLUMN.getBytes());
  public static final BytesWritable columnColumn = new BytesWritable(CassandraColumnSerDe.CASSANDRA_COLUMN_COLUMN.getBytes());
  public static final BytesWritable subColumnColumn = new BytesWritable(CassandraColumnSerDe.CASSANDRA_SUBCOLUMN_COLUMN.getBytes());
  public static final BytesWritable valueColumn = new BytesWritable(CassandraColumnSerDe.CASSANDRA_VALUE_COLUMN.getBytes());



  public CassandraHiveRecordReader(ColumnFamilyRecordReader cfrr, boolean isTransposed)
  {
    this.cfrr = cfrr;
    this.isTransposed = isTransposed;
  }

  @Override
  public void close() throws IOException {
    cfrr.close();
  }

  @Override
  public BytesWritable createKey() {
    return new BytesWritable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return cfrr.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return cfrr.getProgress();
  }

  @Override
  public boolean next(BytesWritable key, MapWritable value) throws IOException {

    if (!nextKeyValue()) {
      return false;
    }

    key.set(getCurrentKey());

    value.clear();
    value.putAll(getCurrentValue());

    return true;
  }

  @Override
  public BytesWritable getCurrentKey()  {
    return currentKey;
  }

  @Override
  public MapWritable getCurrentValue() {
    return currentValue;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    cfrr.initialize(split, context);
  }

  private BytesWritable convertByteBuffer(ByteBuffer val)
  {
    return new BytesWritable(ByteBufferUtil.getArray(val));
  }

  @Override
  public boolean nextKeyValue() throws IOException {

    boolean next = false;

    // In the case that we are transposing we create a fixed set of columns
    // per cassandra column
    if (isTransposed) {
      // This loop is exited almost every time with the break at the end,
      // see DSP-465 note below
      while (true) {
        if ((columnIterator == null || !columnIterator.hasNext()) && (subColumnIterator == null || !subColumnIterator.hasNext())) {
          next = cfrr.nextKeyValue();
          if (next) {
            columnIterator = cfrr.getCurrentValue().entrySet().iterator();
            subColumnIterator = null;
            currentEntry = null;
          } else {
            //More sub columns for super columns.
            if (subColumnIterator != null && subColumnIterator.hasNext()) {
              next = true;
            }
          }
        } else {
          next = true;
        }

        if (next) {
          currentKey = convertByteBuffer(cfrr.getCurrentKey());
          currentValue.clear();
          Map.Entry<ByteBuffer, Column> entry = currentEntry;

          if (subColumnIterator == null || !subColumnIterator.hasNext()) {
            // DSP-465: detect range ghosts and skip this
            if (columnIterator.hasNext()) {
              entry = columnIterator.next();
              currentEntry = entry;
              subColumnIterator = null;
            } else {
              continue;
            }
          }

              // rowKey
              currentValue.put(keyColumn, currentKey);

              // column name
              currentValue.put(columnColumn, convertByteBuffer(currentEntry.getValue().name()));

              currentValue.put(valueColumn, convertByteBuffer(currentEntry.getValue().value()));

          }

        break; //exit ghost row loop
      }
    } else { //untransposed
        next = cfrr.nextKeyValue();

        currentValue.clear();

        if (next) {
            currentKey = convertByteBuffer(cfrr.getCurrentKey());

            // rowKey
            currentValue.put(keyColumn, currentKey);
            populateMap(cfrr.getCurrentValue(), currentValue);
        }
    }

    return next;
  }

  private void populateMap(SortedMap<ByteBuffer, Column> cvalue, MapWritable value)
  {
    for (Map.Entry<ByteBuffer, Column> e : cvalue.entrySet())
    {
      ByteBuffer k = e.getKey();
      Column    v = e.getValue();

      if (!v.isLive(new Date().getTime())) {
        continue;
      }

      BytesWritable newKey   = convertByteBuffer(k);
      BytesWritable newValue = convertByteBuffer(v.value());

      value.put(newKey, newValue);
    }
  }
}
