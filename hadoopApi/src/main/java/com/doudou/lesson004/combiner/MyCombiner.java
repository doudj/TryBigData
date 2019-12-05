package com.doudou.lesson004.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: MyCombiner
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 自定义combiner
 */
public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int mapCombiner = 0;
        for (IntWritable value : values) {
            mapCombiner += value.get();
        }
        context.write(key, new IntWritable(mapCombiner));
        //使用方式，job.setcombinerClass(MyCombiner.class)
    }
}
