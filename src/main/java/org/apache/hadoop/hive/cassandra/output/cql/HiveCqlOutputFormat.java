package org.apache.hadoop.hive.cassandra.output.cql;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.output.Put;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("deprecation")
public class HiveCqlOutputFormat implements HiveOutputFormat<Text, CqlPut>,
        OutputFormat<Text, CqlPut> {

    static final Logger LOG = LoggerFactory.getLogger(HiveCqlOutputFormat.class);

    @Override
    public RecordWriter getHiveRecordWriter(final JobConf jc, Path finalOutPath,
            Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
            Progressable progress) throws IOException {

        final String cassandraKeySpace = jc.get(AbstractCassandraSerDe.CASSANDRA_KEYSPACE_NAME);
        final String cassandraHost = jc.get(AbstractCassandraSerDe.CASSANDRA_HOST);
        final int cassandraPort = Integer.parseInt(jc.get(AbstractCassandraSerDe.CASSANDRA_PORT));

        final CassandraProxyClient client;
        try {
            client = new CassandraProxyClient(
                    cassandraHost, cassandraPort, true, true);
        } catch (CassandraException e) {
            throw new IOException(e);
        }

        return new RecordWriter() {

            @Override
            public void close(boolean abort) throws IOException {
                if (client != null) {
                    client.close();
                }
            }

            @Override
            public void write(Writable w) throws IOException {
                Put put = (Put) w;
                put.write(cassandraKeySpace, client, jc);
            }

        };
    }

    @Override
    public void checkOutputSpecs(FileSystem arg0, JobConf jc) throws IOException {

    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Text, CqlPut> getRecordWriter(FileSystem arg0,
            JobConf arg1, String arg2, Progressable arg3) throws IOException {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }
}
