package minecloseditemsets.hadoop.io;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextArrayWritable extends ArrayWritable implements WritableComparable<TextArrayWritable> {
	
	String[] stringArray;

	public TextArrayWritable() {
		super(Text.class);
	}

	public TextArrayWritable(TextArrayWritable value) {
		this();
		super.set(value.get());
		updateStringArray();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		Writable[] writableArray = super.get();
		this.stringArray = new String[writableArray.length];
		for (int i = 0; i < writableArray.length; i++) {
			this.stringArray[i] = writableArray[i].toString();
		}
	}

	private void updateStringArray() {
		Writable[] writableArray = super.get();
		this.stringArray = new String[writableArray.length];
		for (int i = 0; i < writableArray.length; i++) {
			this.stringArray[i] = writableArray[i].toString();
		}
	}
	
	public String[] toArray() {
		if (this.stringArray == null) {
			updateStringArray();
		}
		return stringArray;
	}
	
	public void setArray(String[] array) {
		Writable[] writable = new Text[array.length];
		for (int i = 0; i < writable.length; i++) {
			writable[i] = new Text(array[i]);
		}
		super.set(writable);
		stringArray = array;
	}

	@Override
	public int compareTo(TextArrayWritable that) {
		for (int i = 0; i < this.stringArray.length; i++) {
			String thisItem = this.stringArray[i];
			if (i > (that.stringArray.length - 1)) {
				return 1;
			}
			String thatItem = that.stringArray[i];
			int compareItems = thisItem.compareTo(thatItem);
			
			if (compareItems != 0) {
				return compareItems;
			} else if ((i == this.stringArray.length - 1) && that.stringArray.length > i + 1  ) {
				return -1;
			}
		}
		return 0;
	}

	@Override
	public String toString() {
		return Arrays.toString(this.stringArray);
	}
	

}
