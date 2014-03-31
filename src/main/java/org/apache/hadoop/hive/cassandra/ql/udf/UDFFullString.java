package org.apache.hadoop.hive.cassandra.ql.udf;

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "full_string",
        value = "_FUNC_([string]) - Returns a full string ")

public class UDFFullString extends UDF {

    public String evaluate(Text text) {
        return new String(text.copyBytes()).replace('\n', ' ');
    }
    
}
