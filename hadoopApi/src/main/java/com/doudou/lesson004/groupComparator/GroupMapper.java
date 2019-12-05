package com.doudou.lesson004.groupComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: GroupMapper
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 自定义mapper类
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        orderBean = new OrderBean();
    }

    /**
     * Order_0000005    Pdt_01  222.8
 *      Order_0000005  Pdt_05  25.8
 *      Order_0000002  Pdt_03  322.8
 *      Order_0000002  Pdt_04  522.4
 *      Order_0000002  Pdt_05  822.4
 *      Order_0000003  Pdt_01  222.8
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        orderBean.setOrderId(splits[0]);
        orderBean.setPrice(Double.valueOf(splits[2]));
        //输出orderBean
        context.write(orderBean, NullWritable.get());
    }
}
