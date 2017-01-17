package minecloseditemsets.app;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yaron on 24/09/15.
 */
public class NullReducer extends Reducer <Object, Object, Object, Object> {
    @Override
    protected void reduce(Object key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
        // Nothing
    }
}
