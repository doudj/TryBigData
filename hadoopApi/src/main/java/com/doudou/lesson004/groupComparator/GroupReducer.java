package com.doudou.lesson004.groupComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: GroupReducer
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 自定义reducer类
 */
public class GroupReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (NullWritable o : values) {
            if (i < 2) {
                context.write(key, NullWritable.get());
                i++;
            } else {
                break;
            }
        }
    }
}
