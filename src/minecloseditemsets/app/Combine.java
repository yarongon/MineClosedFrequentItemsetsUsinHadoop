package minecloseditemsets.app;

import com.google.common.primitives.Ints;
import minecloseditemsets.hadoop.io.IntArrayWritable;
import minecloseditemsets.hadoop.io.ItemsetWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by yaron on 14/10/15.
 */
public class Combine extends Reducer<ClosedItemSet, ItemsetWritable, ClosedItemSet, ItemsetWritable> {
    private static final Log LOG = LogFactory.getLog(Combine.class);

    @Override
    protected void reduce(ClosedItemSet key, Iterable<ItemsetWritable> values, Context context) throws IOException, InterruptedException {
        List<Set<Integer>> transactions = new ArrayList<>();
        int supp = 0;

        /* Convert from Writable String (Text) to plain String */
        for (ItemsetWritable nextTransaction : values) {
            int[] textToString = nextTransaction.getArray();
            List<Integer> transList = Ints.asList(textToString);
            Set<Integer> trans = new HashSet<Integer>(transList);
            transactions.add(trans);
//            transactions.add(ArrayUtils.toObject(textToString));
            supp += nextTransaction.getCount();
        }

        /* Intersect the transactions to find the closure on the key */
        Integer[] closure = Reduce.intersection(transactions);
        IntArrayWritable closureWritable = new IntArrayWritable();
        closureWritable.setArray(closure);

        ItemsetWritable value = new ItemsetWritable();
        value.setItemset(closureWritable.get());
        value.setCount(supp);
        context.write(key, value);
    }


}
