package minecloseditemsets.hadoop.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TextFileTwitterInputFormat extends InputFormat<LongWritable, TextArrayWritable> {
	
	TextInputFormat textInputFormat = new TextInputFormat();

	public static void addInputPath(Job job, Path path) throws IOException {
		TextInputFormat.addInputPath(job, path);		
	}
	

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		return textInputFormat.getSplits(context);
	}

	@Override
	public RecordReader<LongWritable, TextArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new TextFileTwitterRecordReader();
	}


}
