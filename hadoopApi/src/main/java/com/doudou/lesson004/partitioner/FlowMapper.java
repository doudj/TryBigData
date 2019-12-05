package com.doudou.lesson004.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: FlowMapper
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: TODO
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean flowBean;
    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
        text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        String phoneNo = splits[1];
        String upFlow =splits[6];
        String downFlow =splits[7];
        String upCountFlow =splits[8];
        String downCountFlow =splits[9];
        text.set(phoneNo);
        flowBean.setUpFlow(Integer.parseInt(upFlow));
        flowBean.setDownFlow(Integer.parseInt(downFlow));
        flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));
        context.write(text, flowBean);
    }
}
