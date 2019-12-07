package com.doudou.lesson004.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: JoinReducer
 * @Author: doudou
 * @Datetime: 2019/12/7
 * @Description: 自定义reducer
 */
public class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firstPart = "";
        String secondPart = "";

        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                secondPart = value.toString();
            } else {
                firstPart = value.toString();
            }
        }
        context.write(new Text(firstPart + "\t" + secondPart), NullWritable.get());
    }
}
