package org.apache.hadoop.hive.cassandra.ql.udf;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "to_uuid",
    value = "_FUNC_([string]) - Returns a UUID parsed from an input stringa string of 32 hexidecimal characters",
    extended = "Takes a String of 32 hexidecimal characters, \n" +
                "split up using dashes in the standard UUID format:\n" +
                " XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX \n" +
                " and passes it through UUIDGen.decompose(UUID.fromString(str))")

public class UDFUuid extends UDF {

  public BytesWritable evaluate(Text text){
    return new BytesWritable(UUIDGen.decompose(UUID.fromString(new String(text.getBytes()))));
  }

}
