package minecloseditemsets.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TextFileTwitterRecordReader extends RecordReader<LongWritable, TextArrayWritable> {
	
	LineRecordReader lineRecordReader = new LineRecordReader(); 
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lineRecordReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lineRecordReader.nextKeyValue();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return lineRecordReader.getCurrentKey();
	}

	@Override
	public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
		TextArrayWritable res = new TextArrayWritable();
		Text value = lineRecordReader.getCurrentValue();
		String[] valueArray = value.toString().split(" ");
		res.setArray(valueArray);
		
		return res;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}
	

}
