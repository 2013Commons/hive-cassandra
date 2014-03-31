package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraLazyString extends LazyString {

    public static final Logger LOG = LoggerFactory.getLogger(CassandraLazyString.class);
    public CassandraLazyString(LazyStringObjectInspector oi) {
        super(oi);
    }

    public CassandraLazyString(LazyString other) {
        super(other);
        Text incoming = new Text(other.getWritableObject().copyBytes()); 
        data = incoming;
    }

    @Override
    public void init(ByteArrayRef bytes, int start, int length) {
        byte[] recv = new byte[length];
        System.arraycopy(bytes.getData(), start, recv, 0, length);
        String str = new String(recv);
        //str = str.replace("\n", " ");
        data.set(str);
    }

    
    public Text getData() {
        return data;
    }
    
}
