package org.apache.hadoop.hive.serde2.lazy;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class LazyCassandraUtils {

    public static AbstractType getCassandraType(PrimitiveObjectInspector oi) {
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN:
                return BooleanType.instance;
            case INT:
                return Int32Type.instance;
            case LONG:
                return LongType.instance;
            case FLOAT:
                return FloatType.instance;
            case DOUBLE:
                return DoubleType.instance;
            case STRING:
                return UTF8Type.instance;
            case BYTE:
            case SHORT:
            case BINARY:
                return BytesType.instance;
            case TIMESTAMP:
                return DateType.instance;
            default:
                throw new RuntimeException("Hive internal error.");

        }
    }
}
