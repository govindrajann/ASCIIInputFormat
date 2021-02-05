package com.go.hadoop.format.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ASCIIFixedLengthCopybookInputFormat extends FileInputFormat<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(ASCIIFixedLengthCopybookInputFormat.class.getName());

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
                                                            JobConf job, Reporter reporter) throws IOException {

        LOG.info("In ASCIICopybookInputFormat.getRecordReader() method");
        return new ASCIIFixedLengthRecordReader((FileSplit) split, job);

    }
}

