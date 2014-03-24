package org.apache.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;

/**
 * CassandraLazyTimestamp parses the object into TimestampWritable value.
 *
 */
public class CassandraLazyTimestamp extends LazyTimestamp
{
  public CassandraLazyTimestamp(LazyTimestampObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    if ( length == 8 ) {
      try {
        ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
        data.set(new Timestamp(buf.getLong(buf.position())));
        isNull = false;
        return;
      } catch (Throwable ie) {
        //we are unable to parse the data, try to parse it in the hive lazy way.
      }
    }

    super.init(bytes, start, length);
  }
}

