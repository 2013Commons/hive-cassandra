package org.apache.hadoop.hive.cassandra.ql.udf;

import java.math.BigDecimal;

import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "to_decimal",
        value = "_FUNC_([arg]) - Returns a binary representation of a Java BigDecimal",
        extended = "For use with Cassandra decimal values. Takes a string or numeric \n"
        + "argument and returns the binary object")

public class UDFDecimal extends UDF {

    public BytesWritable evaluate(Text text) {
        return new BytesWritable(
                DecimalType.instance.decompose(
                        new BigDecimal(new String(text.getBytes()))).array());
    }

    public BytesWritable evaluate(IntWritable iw) {
        return new BytesWritable(
                DecimalType.instance.decompose(
                        new BigDecimal(iw.get())).array());
    }

    public BytesWritable evaluate(LongWritable lw) {
        return new BytesWritable(
                DecimalType.instance.decompose(
                        new BigDecimal(lw.get())).array());
    }

}
