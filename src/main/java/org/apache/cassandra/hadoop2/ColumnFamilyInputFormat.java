/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one
 * ColumnFamily.
 *
 * At minimum, you need to set the CF and predicate (description of columns to
 * extract from each row) in your Hadoop job Configuration. The ConfigHelper
 * class is provided to make this simple: ConfigHelper.setInputColumnFamily
 * ConfigHelper.setInputSlicePredicate
 *
 * You can also configure the number of rows per InputSplit with
 * ConfigHelper.setInputSplitSize This should be "as big as possible, but no
 * bigger." Each InputSplit is read from Cassandra with multiple get_slice_range
 * queries, and the per-call overhead of get_slice_range is high, so larger
 * split sizes are better -- but if it is too large, you will run out of memory.
 *
 * The default split size is 64k rows.
 */
public class ColumnFamilyInputFormat extends AbstractColumnFamilyInputFormat<ByteBuffer, SortedMap<ByteBuffer, Column>> {

    public RecordReader<ByteBuffer, SortedMap<ByteBuffer, Column>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new ColumnFamilyRecordReader();
    }

    public org.apache.hadoop.mapred.RecordReader<ByteBuffer, SortedMap<ByteBuffer, Column>> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf jobConf, final Reporter reporter) throws IOException {
        TaskAttemptContext tac = new TaskAttemptContextImpl(jobConf, TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID))) {
            @Override
            public void progress() {
                reporter.progress();
            }
        };

        ColumnFamilyRecordReader recordReader = new ColumnFamilyRecordReader(jobConf.getInt(CASSANDRA_HADOOP_MAX_KEY_SIZE, CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT));
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    @Override
    protected void validateConfiguration(Configuration conf) {
        super.validateConfiguration(conf);

        if (org.apache.cassandra.hadoop2.ConfigHelper.getInputSlicePredicate(conf) == null) {
            throw new UnsupportedOperationException("you must set the predicate with setInputSlicePredicate");
        }
    }

}
