package minecloseditemsets.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Combine for finding the frequent <b>items</b>.
 *
 * Created by yaron on 04/06/15.
 */
public class FrequentItemsCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static final Log LOG = LogFactory.getLog(FrequentItemsCombiner.class);
    static IntWritable sum = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum.set(0);

        for (IntWritable count : values) {
            sum.set(sum.get() + count.get());
        }
        context.write(key, sum);
    }

}
