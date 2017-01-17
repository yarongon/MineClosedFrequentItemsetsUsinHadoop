package minecloseditemsets.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yaron on 04/06/15.
 */
public class FrequentItemsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static final Log LOG = LogFactory.getLog(FrequentItemsReducer.class);
    static IntWritable sum = new IntWritable();
    int minSupport;
//    Set<Integer> frequentItems;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.minSupport = context.getConfiguration().getInt(MiningClosedItemSet.MIN_SUPP_PARAMETER_NAME, Integer.MAX_VALUE);
//        this.frequentItems = new HashSet<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum.set(0);

        for (IntWritable count : values) {
            sum.set(sum.get() + count.get());
        }

        if (sum.get() >= minSupport) {
            context.write(key, sum);
//            frequentItems.add(key.get());
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        super.cleanup(context);
//        Configuration conf = context.getConfiguration();
//
//        String fiPath = String.format("output/frequent.items/%s", context.getTaskAttemptID().getTaskID().getId());
//        writeToFile(this.frequentItems, fiPath, conf);
//    }

//    protected void writeToFile(Collection c, String pathString, Configuration conf) throws IOException, InterruptedException {
//        Path outPath = new Path(pathString);
//        FileSystem fs = FileSystem.get(conf);
//        fs.delete(outPath, true);
//
//        //final SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath, ClusterCenter.class, IntWritable.class);
//        FSDataOutputStream out = fs.create(outPath);
//        // FSDataOutputStream out = fs.append(outPath);
//        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
//
//        for (Object object : c) {
//            writer.write(object.toString());
//            writer.write('\n');
//        }
//        writer.close();
//        out.close();
//    }
}
