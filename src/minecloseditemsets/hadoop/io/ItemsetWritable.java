package minecloseditemsets.hadoop.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaron on 14/10/15.
 */
public class ItemsetWritable implements WritableComparable<ItemsetWritable> {
    IntArrayWritable itemset;
    IntWritable count;

    public ItemsetWritable() {
        itemset = new IntArrayWritable();
        count = new IntWritable();
    }

    public ItemsetWritable(IntArrayWritable itemset, IntWritable count) {
        this.itemset = itemset;
        this.count = count;
    }

    public void setItemset(Writable[] itemset) {
        this.itemset.set(itemset);
    }

    public void setCount(int count) {
        this.count.set(count);
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public int getCount() {
        return this.count.get();
    }

    @Override
    public int compareTo(ItemsetWritable o) {
        return itemset.compareTo(o.itemset);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        count.write(dataOutput);
        itemset.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count.readFields(dataInput);
        itemset.readFields(dataInput);
    }

    public int[] getArray() {
        return this.itemset.getArray();
    }
}
