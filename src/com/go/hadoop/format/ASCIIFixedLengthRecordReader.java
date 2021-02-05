package com.go.hadoop.format.mapred;

import org.apache.hadoop.conf.Configuration;
import com.cloudera.sa.copybook.Const;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.Numeric.Convert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ASCIIFixedLengthRecordReader implements RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(ASCIIFixedLengthRecordReader.class.getName());

    private long start;
    private long pos;
    private long end;
    int maxLineLength;
    private int recordByteLength;
    private String fieldDelimiter = new Character((char) 0x01).toString();
    private CompressionCodecFactory compressionCodecs = null;
    private String cblPath;
    StringBuilder strBuilder;
    private LineReader in;
    FileSystem fs;
    Path file;
    private FSDataInputStream fileIn;
    private BufferedInputStream inputStream;
    private ExternalRecord externalRecord;
    private int recProcessed = 0;
    private int byteDiff;

    public static class LineReader extends org.apache.hadoop.util.LineReader {
        LineReader(InputStream in) {
            super(in);
        }

        LineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        public LineReader(InputStream in, Configuration conf) throws IOException {
            super(in, conf);
        }
    }

    public ASCIIFixedLengthRecordReader(FileSplit genericSplit, Configuration job) throws IOException {
        try {

            FileSplit split = (FileSplit) genericSplit;
            LOG.info("In  ASCIIFixedLengthRecordReader Constructor()");
            start = split.getStart();
            end = start + split.getLength();
            fs = FileSystem.get(job);
            cblPath = job.get(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);
            file = split.getPath();
            fileIn = fs.open(split.getPath());
            compressionCodecs = new CompressionCodecFactory(job);
            final CompressionCodec codec = compressionCodecs.getCodec(file);

            boolean skipFirstLine = false;
            if (codec != null) {
                in = new LineReader(codec.createInputStream(fileIn), job);
                end = Long.MAX_VALUE;
            } else {
                if (start != 0) {
                    skipFirstLine = true;
                    --start;
                    fileIn.seek(start);
                }
                in = new LineReader(fileIn, job);
            }
            LOG.info("CopyBook cblPath : " + cblPath);
            // Check cblPath exists
            if (cblPath == null) {
                if (job != null) {
                    MapWork mrwork = Utilities.getMapWork(job);

                    if (mrwork == null) {
                        LOG.info("When running a client side hive job you have to set \"copybook.putformat.cbl.hdfs.path\" before executing the query.");
                        LOG.info("When running a MR job we can get this from the hive TBLProperties");
                    }

                    Map<String, PartitionDesc> map = mrwork.getPathToPartitionInfo();
                    for (Map.Entry<String, PartitionDesc> pathsAndParts : map.entrySet()) {
                        LOG.info("Hey");
                        Properties props = pathsAndParts.getValue().getTableDesc().getProperties();
                        cblPath = props
                                .getProperty(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);
                        break;
                    }
                }
            }
            inputStream = new BufferedInputStream(
                    fs.open(new Path(cblPath)));

            CobolCopybookLoader copybookInt = new CobolCopybookLoader();
            externalRecord = copybookInt
                    .loadCopyBook(inputStream, "RR", CopybookLoader.SPLIT_NONE, 0,
                            "cp037", Convert.FMT_MAINFRAME, 0, null);

            if (skipFirstLine) {
                Text dummy = new Text();

                start += in.readLine(dummy, 0,
                        (int) Math.min(
                                (long) Integer.MAX_VALUE,
                                end - start));
            }
            this.pos = start;
            for (ExternalField field : externalRecord.getRecordFields()) {
                recordByteLength += field.getLen();
            }
            maxLineLength = recordByteLength;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ASCIIFixedLengthRecordReader(InputStream in, long offset, long endOffset,
                                 int maxLineLength) {
        this.maxLineLength = maxLineLength;
        this.in = new LineReader(in);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
    }

    public ASCIIFixedLengthRecordReader(InputStream in, long offset, long endOffset,
                                 Configuration job)
            throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        this.in = new LineReader(in, job);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    /**
     * Read a line.
     */
    public boolean next(LongWritable key, Text value)
            throws IOException {
        LOG.info("next record***************************");
        try {
            if (pos >= end) {
                return false;
            }

            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new Text();
            }

            while (pos < end) {
                int newSize = in.readLine(value, maxLineLength,
                        Math.max((int) Math.min(
                                Integer.MAX_VALUE, end - pos),
                                maxLineLength));
                String inRecord = value.toString();
                if (newSize == 0) {
                    break;
                }

                pos += newSize;
                key.set(pos);

                if (newSize < maxLineLength) {
                    LOG.info("newSize < maxLineLength. newSize :" + newSize + " maxLineLength :" + maxLineLength);
                    byteDiff = (recordByteLength + 1) - newSize;
                    newSize = newSize + byteDiff;
                    if (byteDiff > 0) {
                        inRecord = inRecord.format("%s%" + byteDiff + "s", inRecord, " ");
                    }
                }
                LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
                int currPos = 0;
                int fieldLen = 0;
                strBuilder = new StringBuilder();
                boolean isFirst = true;
                int i = 0;

                if (strBuilder == null) {
                    System.out.println("StringBuilder is empty :" + strBuilder);
                }

                if (newSize == 0) {
                    key = null;
                    value = null;
                    return false;
                }

                // Adding Delimiter
                for (ExternalField field : externalRecord.getRecordFields()) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        strBuilder.append(fieldDelimiter);
                    }
                    fieldLen = field.getLen();
                    strBuilder.append(inRecord.substring(currPos, currPos + fieldLen));
                    currPos = currPos + fieldLen;
                }
                value.set(strBuilder.toString());
                recProcessed++;
                if (newSize != 0) {
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("Error Message is : " + e.getMessage());
        }
        return true;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() throws IOException {
        LOG.info("Calling ASCIIFixedLengthRecordReader getProgress()");
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public long getPos() throws IOException {
        LOG.info("Calling ASCIIFixedLengthRecordReader getPos()");
        return pos;
    }

    public void close() throws IOException {
        LOG.info("Calling ASCIIFixedLengthRecordReader close()");
        if (in != null) {
            System.out.print("Number of records processeed : " + recProcessed);
            in.close();
        }
    }
}

