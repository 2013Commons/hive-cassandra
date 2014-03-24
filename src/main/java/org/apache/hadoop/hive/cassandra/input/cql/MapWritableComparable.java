package org.apache.hadoop.hive.cassandra.input.cql;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MapWritableComparable extends MapWritable implements WritableComparable<MapWritableComparable> {

    @Override
    public int compareTo(MapWritableComparable that) {
        if (this == that) {
            return 0;
        }
        if (this.keySet().size() == that.keySet().size()) {
            for (Entry<Writable, Writable> thisEntry : this.entrySet()) {
                Writable thatValue = that.get(thisEntry.getKey());
                if (!thisEntry.getValue().equals(thatValue)) {
                    return -1;
                }
            }
            //implies all key-value pairs are equal.
            return 0;
        }
        return -1;
    }
}
