package minecloseditemsets.hadoop.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaron on 28/05/15.
 */
public class IntArrayInputFormat extends FileInputFormat<LongWritable, IntArrayWritable> {
    @Override
    public RecordReader<LongWritable, IntArrayWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new IntArrayRecordReader();
    }
}
