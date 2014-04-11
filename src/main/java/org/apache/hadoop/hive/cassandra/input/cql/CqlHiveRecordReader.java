package org.apache.hadoop.hive.cassandra.input.cql;

import org.apache.cassandra.hadoop2.cql3.CqlPagingRecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class CqlHiveRecordReader extends RecordReader<MapWritableComparable, MapWritable>
        implements org.apache.hadoop.mapred.RecordReader<MapWritableComparable, MapWritable> {

    static final Logger LOG = LoggerFactory.getLogger(CqlHiveRecordReader.class);

    //private final boolean isTransposed;
    private final CqlPagingRecordReader cfrr;
    private Iterator<Map.Entry<String, ByteBuffer>> columnIterator = null;
    private Map.Entry<String, ByteBuffer> currentEntry;
    //private Iterator<IColumn> subColumnIterator = null;
    private MapWritableComparable currentKey = null;
    private final MapWritable currentValue = new MapWritable();

    public CqlHiveRecordReader(CqlPagingRecordReader cprr) { //, boolean isTransposed) {
        this.cfrr = cprr;
        //this.isTransposed = isTransposed;
    }

    @Override
    public void close() throws IOException {
        cfrr.close();
    }

    @Override
    public MapWritableComparable createKey() {
        return new MapWritableComparable();
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

    public static int callCount = 0;

    @Override
    public boolean next(MapWritableComparable key, MapWritable value) throws IOException {
        if (!nextKeyValue()) {
            return false;
        }

        key.clear();
        key.putAll(getCurrentKey());

        value.clear();
        value.putAll(getCurrentValue());

        return true;
    }

    @Override
    public MapWritableComparable getCurrentKey() {
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

    
    /**
     * Where we treat the buffer from cassandra, be careful with that, and make sure if
     * you want to support the null column.
     * 
     * @param val
     * @return 
     */
    private BytesWritable convertByteBuffer(ByteBuffer val) {
        if (val == null) {
            return new BytesWritable(new byte[]{});
        }
        int length = val.remaining();
        int boff = val.arrayOffset() + val.position();
        // LOG.debug("size............................"+val.array().length);
        // LOG.debug("buffer............................"+val.toString());
        if (boff == 0 && length == val.array().length) {
            BytesWritable table = new BytesWritable(val.array());
            //  String s2 = new String(table.copyBytes());
            //   LOG.debug("buffer............................"+s2);
            return table;
        } else {
            BytesWritable table = new BytesWritable(Arrays.copyOfRange(val.array(), boff, boff + length));
            // String s2 = new String(table.copyBytes());
            // LOG.debug("buffer............................"+s2);
            return table;
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException {

        boolean next = false;

        // In the case that we are transposing we create a fixed set of columns
        // per cassandra column
        next = cfrr.nextKeyValue();

        currentValue.clear();

        if (next) {
            currentKey = mapToMapWritable(cfrr.getCurrentKey());

            // rowKey
            currentValue.putAll(currentKey);
            currentValue.putAll(mapToMapWritable(cfrr.getCurrentValue()));
            //populateMap(cfrr.getCurrentValue(), currentValue);
        }

        return next;
    }

    private MapWritableComparable mapToMapWritable(Map<String, ByteBuffer> map) {
        MapWritableComparable mw = new MapWritableComparable();
        for (Map.Entry<String, ByteBuffer> e : map.entrySet()) {
            mw.put(new Text(e.getKey()), convertByteBuffer(e.getValue()));
        }
        return mw;
    }

}
