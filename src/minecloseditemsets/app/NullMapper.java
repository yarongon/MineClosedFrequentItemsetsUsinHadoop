package minecloseditemsets.app;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yaron on 24/09/15.
 */
public class NullMapper extends Mapper <Object, Object, Object, Object> {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        // Nothing
    }
}
