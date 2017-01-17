package minecloseditemsets.hadoop.io;

import org.apache.hadoop.mapreduce.Partitioner;

public class TwitterKeyPartitioner extends Partitioner<TwitterKeyWritable, TextArrayWritable> {

	@Override
	public int getPartition(TwitterKeyWritable key, TextArrayWritable value, int numPartitions) {
		return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}


}
