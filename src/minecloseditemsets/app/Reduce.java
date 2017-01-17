package minecloseditemsets.app;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;
import minecloseditemsets.hadoop.io.IntArrayWritable;
import minecloseditemsets.hadoop.io.ItemsetWritable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<ClosedItemSet, ItemsetWritable, Text, Text> {
    private static final Log LOG = LogFactory.getLog(Reduce.class);
    // List<ClosedItemSet> closedItemsets = new ArrayList<ClosedItemSet>();

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
//    }

    public void reduce(ClosedItemSet key, Iterable<ItemsetWritable> values, Context context) throws IOException, InterruptedException {

			/* Accumulate the transactions for this key */
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
		
			/* Get the minimum supp from the context */
        int minSupport = context.getConfiguration().getInt(MiningClosedItemSet.MIN_SUPP_PARAMETER_NAME, Integer.MAX_VALUE);
			
			/* Dismiss keys with less than minimum supp */
        if (supp < minSupport) {
            return;
        }
		
			/* Intersect the transactions to find the closure on the key */
        Integer[] closure = intersection(transactions);

			/* Check if the key for this reducer is lexicographically smaller then the intersection.
			 * if so then this reducer output is valid.
			 * else ignore this reducer result to prevent duplicates. 
			 */
        //if (compareArrays(key.getGenerator(), closure) > 0) {
        //	return;
        //}
        Integer firstItem = addedItemInClosure(key.getItemset(), closure);
        if (firstItem != null) {
            if (Integer.compare(key.getAddedItem(), firstItem) > 0) {
                return;
            } else {
                LOG.info(String.format("YGYG first item: %s, itemset: %s", firstItem, Arrays.toString(key.getItemset())));
            }
        }


        ClosedItemSet newItemset = new ClosedItemSet(key.getGenerator(), closure, supp);
        context.write(new Text(newItemset.toString()), new Text());
        //this.closedItemsets.add(new ClosedItemSet(key.getGenerator(), closure, supp));
        context.getCounter(InternalCounters.REDUCER_OUTPUT).increment(1);
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        super.cleanup(context);
//        Configuration conf = context.getConfiguration();
//        int jobCounter = conf.getInt(MiningClosedItemSet.JOB_COUNTER, 0);
//        Path outputPath = FileOutputFormat.getOutputPath(context);
//
//        String cfiPath = String.format(outputPath.toString() + "/../closed.itemsets.%s/%s", jobCounter, context.getTaskAttemptID().getTaskID().getId());
//        writeToFile(this.closedItemsets, cfiPath, conf);
//    }

//    protected void writeToFile(Collection c, String pathString, Configuration conf) throws IOException, InterruptedException {
//        Path outPath = new Path(pathString);
//        LOG.info("writing to file: " + outPath.toString());
//        FileSystem fs = FileSystem.get(outPath.toUri(), conf);
//        if (fs.exists(outPath)) {
//            fs.delete(outPath, true);
//        }
//
//        FSDataOutputStream out = fs.create(outPath);
//        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
//
//        for (Object object : c) {
//            writer.write(object.toString());
//            writer.write('\n');
//        }
//        writer.close();
//        out.close();
//    }

//    public static Integer[] intersection(List<Integer[]> vector) {
//        if (vector.size() == 1) {
//            return vector.get(0);
//        }
//
//        Set<Integer> tempRes = Arrays.stream(vector.get(0)).collect(Collectors.toSet());
//        for (int i = 1; i < vector.size(); i++) {
//            Set<Integer> currVec = Arrays.stream(vector.get(i)).collect(Collectors.toSet());
//            tempRes.retainAll(currVec);
//        }
//
//        Integer[] res = tempRes.stream().sorted().toArray(Integer[]::new);
//
//        return res;
//    }

    public static Integer[] intersection(List<Set<Integer>> vector) {
        Set<Integer> res;

        if (vector.size() == 1) {
            res =  vector.get(0);
        } else {
            Set<Integer> tempRes = vector.get(0);
            for (int i = 1; i < vector.size(); i++) {
                Set<Integer> currVec = vector.get(i);
                tempRes.retainAll(currVec);
            }

            res = tempRes;
        }

        return res.stream().sorted().toArray(Integer[]::new);
    }

    public static int compareArrays(String[] arr1, String[] arr2) {
        int comp = 0;
        int pos = 0;
        try {
            while (pos < arr1.length & pos < arr2.length) {
                comp = arr1[pos].compareTo(arr2[pos]);
                if (comp != 0) {
                    return comp;
                }
                pos++;
            }

            return arr1.length - arr2.length;

        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return comp;
    }

    public static int compareArrays(Integer[] arr1, Integer[] arr2) {
        int comp = 0;
        int pos = 0;
        try {
            while (pos < arr1.length & pos < arr2.length) {
                comp = Integer.compare(arr1[pos], arr2[pos]);
                if (comp != 0) {
                    return comp;
                }
                pos++;
            }

            return arr1.length - arr2.length;

        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return comp;
    }


    /**
     * Returns the smallest item in the closure not appearing in the itemset. <br>
     * For example, for input closure: {a,c,d,f} and itemset {a,c} returns d.
     *
     * @param itemsetArray
     * @param closureArray
     * @return
     */
    public static Integer addedItemInClosure(Integer[] itemsetArray, Integer[] closureArray) {
        Integer res;
        Set<Integer> itemset = new HashSet<>(Arrays.asList(itemsetArray));
        Set<Integer> closure = new HashSet<>(Arrays.asList(closureArray));

        closure.removeAll(itemset);

        List<Integer> diff = new ArrayList<>(closure);

        if (diff.size() == 0) {
            res = null;
        } else {
            Collections.sort(diff);
            res = diff.get(0);

        }

        return res;
    }

}
	