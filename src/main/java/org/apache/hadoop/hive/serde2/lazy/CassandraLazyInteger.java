package org.apache.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;

/**
 * CassandraLazyInteger parses the object into LongInteger value.
 *
 */
public class CassandraLazyInteger extends LazyInteger {

  public CassandraLazyInteger(LazyIntObjectInspector oi) {
    super(oi);
  }


  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    if ( length == 4 ) {
      try {
        ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
        data.set(buf.getInt(buf.position()));
        isNull = false;
        return;
      } catch (Throwable ie) {
        //we are unable to parse the data, try to parse it in the hive lazy way.
      }
    }

    super.init(bytes, start, length);
  }
}
