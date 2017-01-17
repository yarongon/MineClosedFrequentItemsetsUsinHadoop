package minecloseditemsets.app;

import java.io.*;
import java.net.URI;
import java.util.*;

import minecloseditemsets.hadoop.io.IntArrayWritable;
import minecloseditemsets.hadoop.io.ItemsetWritable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, IntArrayWritable, ClosedItemSet, ItemsetWritable> {
    private static final Integer[] EMPTY_INTS_ARRAY = new Integer[0];
    private static final Log LOG = LogFactory.getLog(Map.class);

    List<ClosedItemSet> closedItemsets = new ArrayList<>();

    @Override
    /**
     * Reads the current closed item-sets from the distributed cache,
     * and loads the field closedItemsets.
     *
     * Also reads the frequent items and loads it them into frequentItems.
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        LOG.info("Reading the cached files...");
        Configuration conf = context.getConfiguration();
        URI[] localFiles = context.getCacheFiles();

        if (localFiles != null) {
            for (URI uri : localFiles) {
                FileSystem fs = FileSystem.get(uri, conf);
                Path path = new Path(uri);
                LOG.info(String.format("Cache: %s",path.toString()));
                BufferedReader fis;
                try {
                    fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                } catch (FileNotFoundException e) {
                    LOG.error(String.format("%s not found", path.toString()));
                    LOG.error(e.getMessage());
                    continue;
                } catch (IllegalArgumentException e) {
                    LOG.error(e.getMessage());
                    continue;
                }
                String line;
                while ((line = fis.readLine()) != null) {
//                if (parentDir.equalsIgnoreCase(MiningClosedItemSet.FREQUENT_ITEMS_PATH)) {
//                    int indexOfTab = line.indexOf('\t');
//                    if (indexOfTab == -1) {
//                        LOG.info(String.format("Line: '%s' does not contain tab character", line));
//                    } else {
//                        String item = line.substring(0,indexOfTab);
//                        frequentItems.add(Integer.parseInt(item));
//                    }
//                } else {
                    ClosedItemSet cis = ClosedItemSet.deserialize(line);
                    closedItemsets.add(cis);
//
//                }
                }
                fis.close();
            }
        }



        if (closedItemsets.isEmpty()) {
            closedItemsets.add(new ClosedItemSet(EMPTY_INTS_ARRAY, EMPTY_INTS_ARRAY, 0));
        }
    }

    /**
     *
     */
    public void map(LongWritable key, IntArrayWritable transaction, Context context) throws IOException, InterruptedException {
        //TextArrayWritable outKey = new TextArrayWritable();

        ItemsetWritable outValue = new ItemsetWritable();
        outValue.setItemset(transaction.get());
        outValue.setCount(1);

        for (ClosedItemSet cfis : this.closedItemsets) {
            Integer[] projected = ArrayUtils.toObject(transaction.getArray());

            // Removing the projector from the items.
            // We must check is the projected contains all the projector.
            List<Integer> transClosureDiff = new ArrayList<>(Arrays.asList(projected));
            int projectedLengthBeforeRemove = transClosureDiff.size();
            transClosureDiff.removeAll(Arrays.asList(cfis.getItemset()));

            if (projectedLengthBeforeRemove != transClosureDiff.size() + cfis.getItemset().length) {
                // the projected does not contain the projector
                continue;
            }

            projected = transClosureDiff.toArray(new Integer[transClosureDiff.size()]);

            int lastItemOfGenerator = -1;
            int generatorLength = cfis.getGenerator().length;
            if (generatorLength > 0) {
                lastItemOfGenerator = cfis.getGenerator()[generatorLength - 1];
            }

            Set<Integer> seenItems = new HashSet<>();

            for (Integer item : projected) {
                if (seenItems.contains(item)) {
                    continue;
                } else {
                    seenItems.add(item);
                }

                // check if item is frequent
//                if (!isItemFrequent(item)) {
//                    context.getCounter(InternalCounters.PRUNED_MESSAGES).increment(1);
//                    continue;
//                }
                // Adding item to the projector
                if (Integer.compare(item, lastItemOfGenerator) > 0) {
                    //if (Reduce.compareArrays(new String[] {item}, projector.getGenerator()) > 0) {
                    // YGYG trying to add to the generator and not the closure.
                    Integer[] cisWithNewItem = appendValue(cfis.getItemset(), item);
                    Integer[] generatorWithNewItem = appendValue(cfis.getGenerator(), item);
                    Arrays.sort(cisWithNewItem);
                    Arrays.sort(generatorWithNewItem);
                    //outKey.setArray(outKeyArray);
                    ClosedItemSet outKey = new ClosedItemSet(generatorWithNewItem, cisWithNewItem, item, 0);

                    context.write(outKey, outValue);
                }
            }
        }


    }

    private Integer[] appendValue(Integer[] obj, int newValue) {

        List<Integer> newObj = new ArrayList<>(Arrays.asList(obj));
        newObj.add(newValue);

        //convert Integer[] to int[]
        return newObj.toArray(new Integer[newObj.size()]);

    }

}
