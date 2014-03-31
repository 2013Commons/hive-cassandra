package org.apache.hadoop.hive.cassandra.ql.udf;

import org.apache.cassandra.utils.Hex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "hex_to_bytes",
        value = "_FUNC_([string]) - Converts a String to the correct binary format for Cassandra blob fields",
        extended = "Takes a String and returns a binary object \n"
        + "after passing it through Hex.hexToBytes()")

public class UDFHexToBytes extends UDF {

    public BytesWritable evaluate(Text text) {
        return new BytesWritable(Hex.hexToBytes(new String(text.getBytes())));
    }

}
