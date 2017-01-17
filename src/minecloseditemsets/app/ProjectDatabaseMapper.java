package minecloseditemsets.app;

import minecloseditemsets.hadoop.io.IntArrayWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class ProjectDatabaseMapper extends Mapper<LongWritable, IntArrayWritable, NullWritable, Text> {
    private static final Log LOG = LogFactory.getLog(ProjectDatabaseMapper.class);

    Set<Integer> frequentItems = new HashSet<>();
    Text valueOut = new Text();

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

        if (localFiles == null) {
            return;
        }

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
                    int indexOfTab = line.indexOf('\t');
                    if (indexOfTab == -1) {
                        LOG.info(String.format("Line: '%s' does not contain tab character", line));
                    } else {
                        String item = line.substring(0, indexOfTab);
                        frequentItems.add(Integer.parseInt(item));
                    }
//                }
            }
            fis.close();
        }

    }

    /**
     *
     */
    public void map(LongWritable key, IntArrayWritable transaction, Context context) throws IOException, InterruptedException {
        Set<Integer> projectedTrans = Arrays.stream(transaction.getArray()).boxed().collect(Collectors.toSet());
        projectedTrans.retainAll(this.frequentItems);
        String outString = projectedTrans.stream().sorted().map(Object::toString).collect(Collectors.joining(" "));
        if (outString.length() > 0) {
            valueOut.set(outString);
            context.write(NullWritable.get(),valueOut);
        }
    }

}
