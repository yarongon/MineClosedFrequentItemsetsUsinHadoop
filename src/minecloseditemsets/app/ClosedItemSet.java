package minecloseditemsets.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class ClosedItemSet implements WritableComparable {
	
	private Integer[] itemset;
	private Integer[] generator;
	private Integer addedItem;
	int support;
	
	public Integer[] getItemset() {
		return itemset;
	}


	public Integer[] getGenerator() {
		return generator;
	}


	public int getSupport() {
		return support;
	}
	
	public Integer getAddedItem() {
		return this.addedItem;
	}

	public ClosedItemSet() {
		
	}
	
	/**
	 * 
	 * @param generator
	 * @param itemset
	 * @param support
	 */
	public ClosedItemSet(Integer[] generator, Integer[] itemset, int support) {
		super();
		this.generator = generator;
		this.itemset = itemset;
		this.support = support;
		this.addedItem = -1;
	}
	
	/**
	 * 
	 * @param generator 
	 *           if created by mapper then generator of previous round with one new item.
	 *           if created by reducer then its the generator.
	 * @param itemset
	 *           if created by mapper then its the cfis from previous round with one new item.
	 *           if created by reducer then the items of the cfis. 
	 * @param addedItem the one new item added
	 * @param support
	 */
	public ClosedItemSet(Integer[] generator, Integer[] itemset, Integer addedItem, int support) {
		super();
		this.generator = generator;
		this.itemset   = itemset;
		this.support   = support;
		this.addedItem = addedItem;
	}


	public String serialize() {
		StringBuilder s = new StringBuilder();
		s.append(String.join(",", Arrays.stream(itemset).map(String::valueOf).toArray(String[]::new)));
		s.append(":");
		s.append(String.join(",",Arrays.stream(generator).map(String::valueOf).toArray(String[]::new)));
		s.append(":");
		s.append(support);
		
		return s.toString();
	}
	
	public static ClosedItemSet deserialize(String lineRead) {

		String line = lineRead.trim();
		String[] helperArray = StringUtils.split(line, ":");
		
		String[] itemset = helperArray[0].split(",");
		String[] generator = helperArray[1].split(",");
		int support = Integer.parseInt(helperArray[2]);
		
		return new ClosedItemSet(
				convertStringArrayToIntArray(generator),
				convertStringArrayToIntArray(itemset),
				support);
	}
	
	@Override
	public String toString() {
//		return "ClosedItemSet [generator=" + Arrays.toString(generator)
//				+ ", itemset=" + Arrays.toString(itemset) + ", support="
//				+ support + "]";
		return serialize();
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		this.generator = convertStringArrayToIntArray(readStringArray(in));
		this.itemset   = convertStringArrayToIntArray(readStringArray(in));
		this.addedItem = in.readInt();
		this.support = in.readInt();
	}


	@Override
	public void write(DataOutput out) throws IOException {
		writeStringArray(convertIntArrayToStringArray(this.generator), out);
		writeStringArray(convertIntArrayToStringArray(this.itemset), out);
		out.writeInt(this.addedItem);
		out.writeInt(this.support);
	}


	@Override
	public int compareTo(Object other) {
		ClosedItemSet otherCIS = (ClosedItemSet)other;
		int retVal = Reduce.compareArrays(this.generator, otherCIS.generator);
		return retVal;
	}

	/**
	 * Write a string array to output.
	 * Handles the length of the array.
	 * 
	 * @param array
	 * @param out
	 * @throws IOException
	 */
	private void writeStringArray(String[] array, DataOutput out) throws IOException {
		out.writeInt(array.length);
		for (String item : array) {
			writeString(item, out);
		}
	}
	
	private void writeString(String str, DataOutput out) throws IOException {
		out.writeInt(str.length());
		out.writeChars(str);
	}
	
	private String[] readStringArray(DataInput in) throws IOException {
		int arraySize = in.readInt();
		String[] readArray = new String[arraySize];
		for (int i = 0; i < readArray.length; i++) {
			readArray[i] = readString(in);
		}
		return readArray;
	}

	private static String[] convertIntArrayToStringArray(Integer[] arr) {
		return Arrays.stream(arr).map(String::valueOf).toArray(String[]::new);
	}

	private static Integer[] convertStringArrayToIntArray(String[] arr) {
		return Arrays.stream(arr).map(Integer::parseInt).toArray(Integer[]::new);
	}
	
	private String readString(DataInput in) throws IOException {
		String str = new String();
		int strLength = in.readInt();
		for (int i = 0; i < strLength; i++) {
			str = str + String.valueOf(in.readChar());
		}
		
		return str;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(generator);
		result = prime * result + Arrays.hashCode(itemset);
		result = prime * result + support;
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClosedItemSet other = (ClosedItemSet) obj;
		if (!Arrays.equals(generator, other.generator))
			return false;
		if (!Arrays.equals(itemset, other.itemset))
			return false;
		return support == other.support;
	}

}
