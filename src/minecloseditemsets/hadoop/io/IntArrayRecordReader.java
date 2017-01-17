package minecloseditemsets.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yaron on 28/05/15.
 */
public class IntArrayRecordReader extends RecordReader<LongWritable, IntArrayWritable> {
    private static final Log LOG = LogFactory.getLog(IntArrayRecordReader.class);
    LineRecordReader lineRecordReader = new LineRecordReader();
    IntArrayWritable currentValue = new IntArrayWritable();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        LOG.info(inputSplit.toString());
        try {
            lineRecordReader.initialize(inputSplit, taskAttemptContext);
        } catch (IOException e) {
            LOG.info("YGYG IOException: " + inputSplit.toString());
            LOG.info("YGYG IOException: " + Arrays.toString(inputSplit.getLocations()));
            throw e;
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean ret = lineRecordReader.nextKeyValue();

        if (ret) {
            Text value = lineRecordReader.getCurrentValue();
            String[] valueArray = value.toString().split(" ");

            Integer[] intArray;
            try {
                intArray = Arrays.stream(valueArray).map(String::trim).map(Integer::parseInt).toArray(Integer[]::new);
            } catch (NumberFormatException e) {
                return this.nextKeyValue();
            }

            currentValue.setArray(intArray);
        }

        return ret;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentKey();
    }

    @Override
    public IntArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return this.currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
