package org.apache.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;

import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;

/**
 * CassandraLazyLong parses the object into BooleanWritable value.
 *
 */
public class CassandraLazyBoolean extends LazyBoolean {

    public CassandraLazyBoolean(LazyBooleanObjectInspector oi) {
        super(oi);
    }

    @Override
    public void init(ByteArrayRef bytes, int start, int length) {

        if (length == 1) {
            try {
                ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
                data.set(BooleanSerializer.instance.deserialize(buf));
                isNull = false;
                return;
            } catch (Throwable ie) {
                isNull = true;
            }
        }

        super.init(bytes, start, length);
    }

}
