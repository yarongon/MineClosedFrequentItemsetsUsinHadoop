package minecloseditemsets.app;

import com.sampullara.cli.Args;
import minecloseditemsets.hadoop.io.IntArrayInputFormat;
import minecloseditemsets.hadoop.io.ItemsetWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MiningClosedItemSet extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(MiningClosedItemSet.class);
	
	public static final String MIN_SUPP_PARAMETER_NAME = "minSup";
	public static final String JOB_NAME = "Mine Closed Itemsets on Tweets";
	
	public static final String CLOSED_ITEMSETS_PATH_ATTR = "closed.itemsets.path";
	public static final String CLOSED_ITEMSETS_PATH = "closed.itemsets";
	public static final String FREQUENT_ITEMS_PATH = "frequent.items";
	public static final String JOB_COUNTER = "job.counter";

	public static final String JAR_PATH = "http://s3.amazonaws.com/ygyg.closed.itemsets/MineClosedItemsetsUsingMR.jar";
	
	private long totalCommunicationCost = 0;
	private long totalNumOfPrunedMessages = 0;
	private long totalNumOfClosedItemsets = 0;
	/*
	public static final String RESULT_STRING = "result_closed_itemset";
	public static final Text   RESULT_TEXT = new Text(RESULT_STRING);
	*/
	
	public static final String CLOSED_ITEMSETS_SEPERATOR = ":";
	
	private long countNumOfTransaction(CommandLineArgs cla) throws Exception {
		Job job = Job.getInstance(this.getConf());
		Configuration conf = job.getConfiguration();

		conf.set("mapred.child.java.opts", "-Xmx4g");

		job.setJobName("Count the number of transactions");
		job.setJarByClass(MiningClosedItemSet.class);
		job.setMapperClass(NullMapper.class);
		job.setReducerClass(NullReducer.class);
		TextInputFormat.addInputPath(job, new Path(cla.inputFileName));
		
		Path outputPath = new Path("/tmp/1.1");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}		
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter mapInputRecordsCounter = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS);

		return mapInputRecordsCounter.getValue();
	}
	
	private void setInput(Job job, String inputFileName) throws IOException {

		if (inputFileName != null) {
			LOG.info("YGYG setting input: " + inputFileName);
			IntArrayInputFormat.addInputPath(job, new Path(inputFileName));
			job.setInputFormatClass(IntArrayInputFormat.class);
		} else {
			LOG.info("YGYG input file name is null.");
		}
	}

	public int run(String[] args) throws Exception {
		// General stuff
	    CommandLineArgs cla = new CommandLineArgs();
	    try {
	    	Args.parse(cla, args);	    	
	    } catch (IllegalArgumentException e) {
	    	Args.usage(cla);
	    	System.exit(-1);
	    }

		Path outputPath = new Path(cla.outputPathName);
		FileSystem fs = FileSystem.get(outputPath.toUri(), this.getConf());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}		
		
		long numOfItemsets = countNumOfTransaction(cla);
		double minSupport = numOfItemsets * cla.minSupport;
		int suppurtValue = (int) Math.ceil(minSupport);
		LOG.info(String.format("Number of itemsets it %d. Minimum items for support is %d", numOfItemsets, suppurtValue));

		String frequentItemsDirectoryName = String.format("%s/%s/", cla.outputPathName, MiningClosedItemSet.FREQUENT_ITEMS_PATH);
		long numOfFrequentItems = findFrequentItems(suppurtValue, cla.inputFileName, frequentItemsDirectoryName);
		LOG.info(String.format("Number of itemsets it %d. Minimum items for support is %d", numOfItemsets, suppurtValue));

		String projectedDatabasePathName = String.format("%s.projected/", cla.inputFileName);
		projectDatabase(cla.inputFileName, frequentItemsDirectoryName, projectedDatabasePathName);

		boolean endProgram = false;
		int jobCounter = 1;
		long currNumOfClosedItemsets = 0;
		long currNumOfPrunedMessages = 0;


		while (!endProgram) {
			Job job = Job.getInstance(this.getConf());
			Configuration conf = job.getConfiguration();

			conf.set("mapred.child.java.opts", "-Xmx4g");
			conf.setBoolean("mapreduce.map.output.compress", true);
			conf.setBoolean("mapreduce.output.fileoutputformat.compress", false);
			conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
			conf.setInt("mapreduce.task.io.sort.factor", 50);
			conf.setInt("mapreduce.task.io.sort.mb", 500);
			conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.8);

			conf.setInt(MIN_SUPP_PARAMETER_NAME, suppurtValue);
			conf.setInt(JOB_COUNTER, jobCounter);		
					
			job.setJarByClass(MiningClosedItemSet.class);
			job.setJobName(String.format("%s job number %d", JOB_NAME, jobCounter));
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(ClosedItemSet.class);
			job.setMapOutputValueClass(ItemsetWritable.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Combine.class);
			job.setReducerClass(Reduce.class);
			
			
			// Sets the input according to the command line flag useCache.
			setInput(job, projectedDatabasePathName);
									
			String closedItemsetsDirectoryName = String.format("%s/%s.%s/", cla.outputPathName, MiningClosedItemSet.CLOSED_ITEMSETS_PATH, (jobCounter-1));
			addDirectoryToCache(fs, job, closedItemsetsDirectoryName);
//			addDirectoryToCache(fs, job, frequentItemsDirectoryName);

			// job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			// String cfiPath = String.format(outputPath.toString() + "/../closed.itemsets.%s/%s", jobCounter, context.getTaskAttemptID().getTaskID().getId());
			// String outputDirName = cla.outputPathName + "/out" + "." + jobCounter + "/";
			String outputDirName = String.format("%s/%s.%s/", cla.outputPathName, MiningClosedItemSet.CLOSED_ITEMSETS_PATH, (jobCounter));
			outputPath = new Path(outputDirName);
			
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			LOG.info("output path: " + outputPath.toString());
			
			FileOutputFormat.setOutputPath(job, outputPath);

			//job.waitForCompletion(true);

			if(!job.waitForCompletion(true)) {
				System.err.println("Something happened.");
			}
			
			currNumOfClosedItemsets = job.getCounters().findCounter(InternalCounters.REDUCER_OUTPUT).getValue();
			currNumOfPrunedMessages = job.getCounters().findCounter(InternalCounters.PRUNED_MESSAGES).getValue();
			LOG.info(String.format("Iteration number: %d, number of closed itemsets: %d, number of pruned candidates: %d", jobCounter, currNumOfClosedItemsets, currNumOfPrunedMessages));
			
			if (currNumOfClosedItemsets == 0) {
				endProgram = true;
			} else {
				jobCounter++;
				this.totalCommunicationCost += job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
				this.totalCommunicationCost += job.getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
				this.totalNumOfPrunedMessages += currNumOfPrunedMessages;
				this.totalNumOfClosedItemsets += currNumOfClosedItemsets;

			}

		}


		LOG.info("Total communication cost: " + this.totalCommunicationCost);
		LOG.info("Total number of pruned messages: " + this.totalNumOfPrunedMessages);
		LOG.info("Total number of closed itemsets: " + this.totalNumOfClosedItemsets);
		return 0;
	}

	private void projectDatabase(String inputPathName, String frequentItemsPathName, String outputPathName) throws Exception {
		Job job = Job.getInstance(this.getConf());
		Configuration conf = job.getConfiguration();

		conf.set("mapred.child.java.opts", "-Xmx4g");

		job.setJarByClass(ProjectDatabaseMapper.class);

		job.setMapperClass(ProjectDatabaseMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJobName("Project database by frequent items.");
		setInput(job, inputPathName);

		Path outputPath = new Path(outputPathName);
		FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		addDirectoryToCache(fs, job, frequentItemsPathName);

		job.waitForCompletion(true);
	}

	private long findFrequentItems(int minSupport, String inputFileName, String frequentItemsDirectoryName) throws  Exception{
		Job job = Job.getInstance(this.getConf());
		Configuration conf = job.getConfiguration();

		conf.set("mapred.child.java.opts", "-Xmx4g");

		job.setJarByClass(FrequentItemsMapper.class);

		job.setMapperClass(FrequentItemsMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(FrequentItemsCombiner.class);
		job.setReducerClass(FrequentItemsReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		conf.setInt(MIN_SUPP_PARAMETER_NAME, minSupport);


		job.setJobName("Find frequent <b>items</b>.");
		setInput(job, inputFileName);

		Path outputPath = new Path(frequentItemsDirectoryName);
		FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);

		Counters counters = job.getCounters();
		Counter reduceOutputRecordsCounter = counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);

		return reduceOutputRecordsCounter.getValue();
	}

	private void addDirectoryToCache(FileSystem fs, Job job, String directoryName) throws IOException, URISyntaxException {
		Path path = new Path(directoryName);
		if (fs.exists(path)) {
            FileStatus[] status = fs.listStatus(path);
            for (FileStatus fileStatus : status) {
				if (!fileStatus.isDirectory()) {
					String uriStr = fileStatus.getPath().toString();
					job.addCacheFile(new URI(uriStr));
					// DistributedCache.addCacheFile(new URI(uriStr), conf);
				}
            }
        }
	}

	public static void main(String[] args) throws Exception {
		LOG.info("Starting mining closed itemsets in MapReduce...");
		long startTime = System.currentTimeMillis();
		int ret = ToolRunner.run(new MiningClosedItemSet(), args);
		long stopTime = System.currentTimeMillis();
		LOG.info("Running time in ms: " + (stopTime - startTime));
		System.exit(ret);
	}

}