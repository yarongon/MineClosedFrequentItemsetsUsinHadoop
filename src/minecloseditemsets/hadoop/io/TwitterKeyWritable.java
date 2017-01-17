/**
 * 
 */
package minecloseditemsets.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author yaron
 *
 */
public class TwitterKeyWritable implements Writable {
	
	TextArrayWritable seed;
	TextArrayWritable key;

	public TwitterKeyWritable() {

	}


	public TwitterKeyWritable(TextArrayWritable seed, TextArrayWritable key) {
		super();
		this.seed = seed;
		this.key = key;
	}

	public TextArrayWritable getSeed() {
		return seed;
	}

	public void setSeed(TextArrayWritable seed) {
		this.seed = seed;
	}

	public TextArrayWritable getKey() {
		return key;
	}

	public void setKey(TextArrayWritable key) {
		this.key = key;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		seed.write(out);
		key.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		seed.readFields(in);
		key.readFields(in);
	}

}
