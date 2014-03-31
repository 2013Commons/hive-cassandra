package org.apache.hadoop.hive.cassandra.ql.udf;

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "uuid_to_string",
        value = "_FUNC_([uuid]) - Returns a string parsed from an input bytes uuid ")

public class UDFUuidToString extends UDF {

    public String evaluate(Text text) {
        return UUID.nameUUIDFromBytes(text.copyBytes()).toString();
    }
    
}
