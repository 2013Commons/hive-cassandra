package org.apache.hadoop.hive.cassandra.ql.udf;

import java.math.BigInteger;

import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "to_varint",
    value = "_FUNC_([arg]) - Returns a binary representation of a Java BigInteger",
    extended = "For use with Cassandra varint values. Takes a string or numeric \n" +
                 "argument and returns the binary object")

public class UDFVarint extends UDF {

  public BytesWritable evaluate(Text text){
    return new BytesWritable(
                  IntegerType.instance.decompose(
                      new BigInteger(new String(text.getBytes()))).array());
  }

  public BytesWritable evaluate(IntWritable iw){
    return new BytesWritable(
                  IntegerType.instance.decompose(
                      BigInteger.valueOf(iw.get())).array());
  }

  public BytesWritable evaluate(LongWritable lw){
    return new BytesWritable(
                  IntegerType.instance.decompose(
                      BigInteger.valueOf(lw.get())).array());
  }

}
