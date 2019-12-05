package com.doudou.lesson003.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: NLineMapper
 * @Author: doudou
 * @Datetime: 2019/12/3
 * @Description: TODO
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        for (String s1 : s) {
            context.write(new Text(s1),new LongWritable(1));
        }
    }
}
