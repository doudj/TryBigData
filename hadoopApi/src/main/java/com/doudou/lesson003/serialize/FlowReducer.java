package com.doudou.lesson003.serialize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: FlowReducer
 * @Author: doudou
 * @Datetime: 2019/12/3
 * @Description: 自定义reduce类
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + upCountFlow + "\t" + downCountFlow));
    }
}
