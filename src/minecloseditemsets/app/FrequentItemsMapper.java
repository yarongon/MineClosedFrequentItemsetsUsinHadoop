package minecloseditemsets.app;

import java.io.IOException;
import java.util.Arrays;

import minecloseditemsets.hadoop.io.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequentItemsMapper extends Mapper<LongWritable, IntArrayWritable, IntWritable, IntWritable> {
	static final IntWritable ONE = new IntWritable(1);
	static IntWritable outItem = new IntWritable();

	@Override
	protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
		int[] items = value.getArray();
		for (int item : value.getArray()) {
			outItem.set(item);
			context.write(outItem,ONE);
		}
	}
}
