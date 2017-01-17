package minecloseditemsets.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Created by yaron on 27/05/15.
 */
public class IntArrayWritable extends ArrayWritable implements WritableComparable<IntArrayWritable> {

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    @Override
    public int compareTo(IntArrayWritable o) {
        return 0;
    }

    public void setArray(Integer[] ints) {
        IntWritable[] intWritables = new IntWritable[ints.length];
        intWritables = Arrays.stream(ints).map(IntWritable::new).toArray(IntWritable[]::new);
        this.set(intWritables);
    }

    public int[] getArray() {
        int[] ints;
        ints = Arrays.stream(this.get()).mapToInt(intW -> ((IntWritable)intW).get()).toArray();
        return ints;
    }

}
