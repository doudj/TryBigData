package com.doudou.lesson004.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: FlowSortMapper
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 自定义mapper
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
    private FlowSortBean flowSortBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        flowSortBean.setPhoneNo(splits[0]);
        flowSortBean.setUpFlow(Integer.valueOf(splits[1]));
        flowSortBean.setDownFlow(Integer.valueOf(splits[2]));
        flowSortBean.setUpCountFlow(Integer.valueOf(splits[3]));
        flowSortBean.setDownCountFlow(Integer.valueOf(splits[4]));
        context.write(flowSortBean, NullWritable.get());
    }
}
