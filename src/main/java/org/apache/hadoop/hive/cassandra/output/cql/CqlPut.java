package org.apache.hadoop.hive.cassandra.output.cql;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.output.CassandraAbstractPut;
import org.apache.hadoop.hive.cassandra.serde.AbstractCassandraSerDe;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This represents a standard column family. It implements hadoop Writable
 * interface.
 */
public class CqlPut extends CassandraAbstractPut implements Writable {

    private ByteBuffer key;
    private List<CqlColumn> columns;

    public CqlPut() {
        columns = new ArrayList<CqlColumn>();
    }

    public CqlPut(ByteBuffer key) {
        this();
        this.key = key;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int keyLen = in.readInt();
        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        key = ByteBuffer.wrap(keyBytes);
        int ilevel = in.readInt();
        int cols = in.readInt();
        for (int i = 0; i < cols; i++) {
            CqlColumn cc = new CqlColumn();
            cc.readFields(in);
            columns.add(cc);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(key.remaining());
        out.write(ByteBufferUtil.getArray(key));
        out.writeInt(1);
        out.writeInt(columns.size());
        for (CqlColumn c : columns) {
            c.write(out);
        }
    }

    public ByteBuffer getKey() {
        return key;
    }

    public void setKey(ByteBuffer key) {
        this.key = key;
    }

    public List<CqlColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<CqlColumn> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("key: ");
        sb.append(this.key);
        for (CqlColumn col : this.columns) {
            sb.append("column : [");
            sb.append(col.toString());
            sb.append("]");
        }
        return sb.toString();
    }

    @Override
    public void write(String keySpace, CassandraProxyClient client, JobConf jc) throws IOException {
        ConsistencyLevel flevel = getConsistencyLevel(jc);

        List<ByteBuffer> values = new ArrayList<ByteBuffer>();

        StringBuilder valuesBuilder = new StringBuilder(" VALUES (");
        StringBuilder queryBuilder = new StringBuilder("INSERT INTO ");
        queryBuilder.append(jc.get(AbstractCassandraSerDe.CASSANDRA_CF_NAME));
        queryBuilder.append("(");
        Iterator<CqlColumn> iter = columns.iterator();
        while (iter.hasNext()) {
            CqlColumn column = iter.next();
            String columnName = new String(column.getColumn());
            queryBuilder.append(columnName);
            valuesBuilder.append("?");
            values.add(ByteBuffer.wrap(column.getValue()));
            if (iter.hasNext()) {
                queryBuilder.append(",");
                valuesBuilder.append(",");
            }
        }
        queryBuilder.append(")");
        valuesBuilder.append(")");

        queryBuilder.append(valuesBuilder);

        try {
            //tODO check compression
            client.getProxyConnection().set_keyspace(keySpace);
            CqlPreparedResult result = client.getProxyConnection().prepare_cql3_query(ByteBufferUtil.bytes(queryBuilder.toString()), Compression.NONE);
            client.getProxyConnection().execute_prepared_cql3_query(result.itemId, values, flevel);
        } catch (InvalidRequestException e) {
            throw new IOException(e);
        } catch (TException e) {
            throw new IOException(e);
        }
    }
}
