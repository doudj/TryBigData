package com.doudouj.lesson003.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: NLineReducer
 * @Author: doudou
 * @Datetime: 2019/12/3
 * @Description: TODO
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long result = 0L;
        for (LongWritable value : values) {
            result +=value.get();
        }
        context.write(key,new LongWritable(result));
    }
}
